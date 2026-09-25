"""BATCH LAYER - Spark batch job, run once per simulated day by Airflow.

Recomputes the accurate picture for one finished simulated day:

    earnings  <- Parquet lake  (the master dataset, all events, nothing dropped)
    costs     <- expenses_<date>.csv  (the daily file from garages/fuel partners)
    profit    =  earnings - (fuel_cost + maintenance_cost)

Writes daily_vehicle_profitability, raises UNPROFITABLE alerts, and produces
the reconciliation report files.

    spark-submit --packages org.postgresql:postgresql:42.7.3 \
        batch/profitability_job.py --date 2026-01-01
"""
import argparse
import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE_PATH = os.getenv("LAKE_PATH", "/opt/app/data/lake/raw/telemetry")
LANDING_DIR = os.getenv("LANDING_DIR", "/opt/app/data/landing")
REPORT_DIR = os.getenv("REPORT_DIR", "/opt/app/data/reports")

JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/fleet")
PG_USER = os.getenv("PG_USER", "fleet")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fleet")
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}


def execute_sql(spark, sql: str):
    conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)
    try:
        st = conn.createStatement()
        st.execute(sql)
        st.close()
    finally:
        conn.close()


def write_report(rows, date: str):
    """Write the reconciliation report as CSV and a simple HTML table."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    cols = ["vehicle_id", "trips", "earnings", "distance_covered", "fuel_cost",
            "maintenance_cost", "total_cost", "profit", "profit_margin",
            "service_flag", "is_unprofitable"]

    csv_path = os.path.join(REPORT_DIR, f"profitability_{date}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r[c] for c in cols})

    losers = [r for r in rows if r["is_unprofitable"]]
    fleet_profit = sum(r["profit"] for r in rows)
    fleet_earnings = sum(r["earnings"] for r in rows)
    fleet_cost = sum(r["total_cost"] for r in rows)

    body = "\n".join(
        "<tr class='{cls}'><td>{v}</td><td>{t}</td><td>{e:,.2f}</td><td>{c:,.2f}</td>"
        "<td>{p:,.2f}</td><td>{m}</td></tr>".format(
            cls="loss" if r["is_unprofitable"] else "",
            v=r["vehicle_id"], t=r["trips"], e=r["earnings"], c=r["total_cost"],
            p=r["profit"], m=("-" if r["profit_margin"] is None else f"{r['profit_margin']:.1%}"))
        for r in rows)

    html_path = os.path.join(REPORT_DIR, f"profitability_{date}.html")
    with open(html_path, "w") as f:
        f.write(f"""<!doctype html><meta charset="utf-8">
<title>Fleet profitability {date}</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:2rem;color:#222}}
 table{{border-collapse:collapse;margin-top:1rem}}
 th,td{{border:1px solid #ccc;padding:.4rem .7rem;text-align:right}}
 th:first-child,td:first-child{{text-align:left}}
 tr.loss{{background:#fdecea}}
 .kpi{{display:inline-block;margin-right:2rem}}
</style>
<h1>Daily per-vehicle profitability</h1>
<p>Simulated day <b>{date}</b> &middot; all amounts in LKR</p>
<p>
 <span class="kpi">Earnings: <b>{fleet_earnings:,.2f}</b></span>
 <span class="kpi">Costs: <b>{fleet_cost:,.2f}</b></span>
 <span class="kpi">Fleet profit: <b>{fleet_profit:,.2f}</b></span>
 <span class="kpi">Unprofitable vehicles: <b>{len(losers)}</b> of {len(rows)}</span>
</p>
<table>
<tr><th>Vehicle</th><th>Trips</th><th>Earnings</th><th>Cost</th><th>Profit</th><th>Margin</th></tr>
{body}
</table>
<p style="color:#666;font-size:.9rem">Rows highlighted in red made a loss on this day.</p>
""")
    return csv_path, html_path, len(losers), fleet_profit


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="simulated date to reconcile (YYYY-MM-DD)")
    args = ap.parse_args()
    date = args.date

    spark = (SparkSession.builder
             .appName(f"fleet-batch-profitability-{date}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 1. earnings from the master dataset ----------
    # Partition pruning: only the one date folder is read, not the whole lake.
    day = (spark.read.parquet(LAKE_PATH)
           .filter(F.col("event_date") == F.to_date(F.lit(date))))

    # Batch layer can afford EXACT distinct counts; the speed layer had to
    # approximate because Spark streaming does not allow countDistinct.
    trips = (day.filter(F.col("fare").isNotNull())
             .groupBy("vehicle_id")
             .agg(F.countDistinct("trip_id").alias("trips"),
                  F.sum("fare").alias("earnings")))

    # ---------- 2. costs from the daily expense file ----------
    expenses_path = os.path.join(LANDING_DIR, f"expenses_{date}.csv")
    expenses = (spark.read.option("header", True).option("inferSchema", True)
                .csv(expenses_path)
                .withColumn("service_flag", F.lower(F.col("service_flag").cast("string")) == "true"))

    # ---------- 3. join and compute profit ----------
    # full outer: a vehicle with costs but no trips is exactly the case we care about
    joined = (expenses.join(trips, on="vehicle_id", how="full_outer")
              .withColumn("trips", F.coalesce(F.col("trips"), F.lit(0)))
              .withColumn("earnings", F.round(F.coalesce(F.col("earnings"), F.lit(0.0)), 2))
              .withColumn("fuel_cost", F.coalesce(F.col("fuel_cost"), F.lit(0.0)))
              .withColumn("maintenance_cost", F.coalesce(F.col("maintenance_cost"), F.lit(0.0)))
              .withColumn("total_cost", F.round(F.col("fuel_cost") + F.col("maintenance_cost"), 2))
              .withColumn("profit", F.round(F.col("earnings") - F.col("total_cost"), 2))
              .withColumn("profit_margin",
                          F.when(F.col("earnings") > 0,
                                 F.round(F.col("profit") / F.col("earnings"), 4)))
              .withColumn("is_unprofitable", F.col("profit") < 0)
              .withColumn("report_date", F.to_date(F.lit(date)))
              .select("report_date", "vehicle_id", "trips", "earnings", "distance_covered",
                      "fuel_cost", "maintenance_cost", "total_cost", "profit",
                      "profit_margin", "service_flag", "is_unprofitable"))

    joined.write.jdbc(JDBC_URL, "stg_daily_profitability", mode="overwrite", properties=PG_PROPS)

    # Idempotent: re-running the same day overwrites its rows instead of duplicating.
    execute_sql(spark, """
        INSERT INTO daily_vehicle_profitability
            (report_date, vehicle_id, trips, earnings, distance_covered, fuel_cost,
             maintenance_cost, total_cost, profit, profit_margin, service_flag,
             is_unprofitable, computed_at)
        SELECT report_date, vehicle_id, trips, earnings, distance_covered, fuel_cost,
               maintenance_cost, total_cost, profit, profit_margin,
               COALESCE(service_flag, FALSE), is_unprofitable, NOW()
        FROM stg_daily_profitability
        ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
            trips = EXCLUDED.trips,
            earnings = EXCLUDED.earnings,
            distance_covered = EXCLUDED.distance_covered,
            fuel_cost = EXCLUDED.fuel_cost,
            maintenance_cost = EXCLUDED.maintenance_cost,
            total_cost = EXCLUDED.total_cost,
            profit = EXCLUDED.profit,
            profit_margin = EXCLUDED.profit_margin,
            service_flag = EXCLUDED.service_flag,
            is_unprofitable = EXCLUDED.is_unprofitable,
            computed_at = NOW();
    """)

    # ---------- 4. alert on vehicles that lost money ----------
    execute_sql(spark, f"""
        INSERT INTO alerts (alert_type, severity, vehicle_id, zone, message, sim_time)
        SELECT 'UNPROFITABLE', 'CRITICAL', d.vehicle_id, NULL,
               'Lost ' || ROUND(ABS(d.profit)) || ' LKR on ' || d.report_date ||
               ' (' || d.trips || ' trips)',
               d.report_date::timestamp
        FROM daily_vehicle_profitability d
        WHERE d.report_date = DATE '{date}'
          AND d.is_unprofitable
          AND NOT EXISTS (
                SELECT 1 FROM alerts a
                WHERE a.vehicle_id = d.vehicle_id
                  AND a.alert_type = 'UNPROFITABLE'
                  AND a.sim_time = d.report_date::timestamp);
    """)

    # ---------- 5. report files ----------
    result = (spark.read.jdbc(
        JDBC_URL,
        f"(SELECT * FROM daily_vehicle_profitability WHERE report_date = DATE '{date}') t",
        properties=PG_PROPS).orderBy("profit"))
    rows = [r.asDict() for r in result.collect()]
    for r in rows:                       # numeric -> float for formatting
        for k in ("earnings", "fuel_cost", "maintenance_cost", "total_cost",
                  "profit", "profit_margin", "distance_covered"):
            r[k] = float(r[k]) if r[k] is not None else None

    csv_path, html_path, losers, fleet_profit = write_report(rows, date)

    execute_sql(spark, f"""
        INSERT INTO pipeline_heartbeat (component, last_seen, status, records_last, details)
        VALUES ('batch_job', NOW(), 'OK', {len(rows)},
                'day {date}: {losers} unprofitable, fleet profit {fleet_profit:.2f}')
        ON CONFLICT (component) DO UPDATE SET
            last_seen = NOW(), status = 'OK',
            records_last = EXCLUDED.records_last, details = EXCLUDED.details;
    """)

    print(f"[batch] {date}: {len(rows)} vehicles, {losers} unprofitable, "
          f"fleet profit {fleet_profit:,.2f} LKR", flush=True)
    print(f"[batch] report: {csv_path} and {html_path}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()