"""Daily reconciliation DAG (BATCH LAYER orchestration).

    wait for expense file -> validate it -> Spark batch job -> check results

The schedule is driven by the SIMULATED clock: one simulated day lasts 24 real
minutes, so the DAG runs every 12 real minutes and reconciles the most recent
finished simulated day. The Spark job is idempotent, so re-running a day is safe.
"""
import csv
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.insert(0, "/opt/app")
from common.sim_clock import SimClock          # noqa: E402  (shared simulated clock)

LANDING_DIR = os.getenv("LANDING_DIR", "/opt/app/data/landing")
EXPECTED_COLUMNS = ["vehicle_id", "fuel_cost", "maintenance_cost",
                    "distance_covered", "service_flag"]
EXPECTED_ROWS = 20


def sim_yesterday() -> str:
    """The most recent FINISHED simulated day."""
    return (SimClock().now().date() - timedelta(days=1)).strftime("%Y-%m-%d")


def validate_expenses(date: str, **_):
    """Data-quality gate: fail loudly before the file reaches the Spark job."""
    path = os.path.join(LANDING_DIR, f"expenses_{date}.csv")
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise AirflowFailException(f"{path} is empty")
    if list(rows[0].keys()) != EXPECTED_COLUMNS:
        raise AirflowFailException(f"unexpected columns: {list(rows[0].keys())}")

    problems = []
    seen = set()
    for r in rows:
        vid = r["vehicle_id"]
        if vid in seen:
            problems.append(f"duplicate vehicle {vid}")
        seen.add(vid)
        for col in ("fuel_cost", "maintenance_cost", "distance_covered"):
            try:
                if float(r[col]) < 0:
                    problems.append(f"{vid}: negative {col}")
            except ValueError:
                problems.append(f"{vid}: {col} is not a number ({r[col]!r})")

    if problems:
        raise AirflowFailException("; ".join(problems[:10]))
    if len(rows) != EXPECTED_ROWS:
        print(f"WARNING: {len(rows)} rows, expected {EXPECTED_ROWS}")

    print(f"validated {path}: {len(rows)} rows, {len(seen)} distinct vehicles")
    return len(rows)


def check_results(date: str, **_):
    """Confirm the batch job actually wrote the day, and log the headline numbers."""
    import psycopg2
    dsn = os.getenv("PG_DSN", "postgresql://fleet:fleet@postgres:5432/fleet")
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(profit), 0),
                   COUNT(*) FILTER (WHERE is_unprofitable)
            FROM daily_vehicle_profitability WHERE report_date = %s
        """, (date,))
        n, profit, losers = cur.fetchone()
        if n == 0:
            raise AirflowFailException(f"no profitability rows written for {date}")
        cur.execute("""
            SELECT vehicle_id, profit FROM daily_vehicle_profitability
            WHERE report_date = %s AND is_unprofitable ORDER BY profit
        """, (date,))
        for vid, p in cur.fetchall():
            print(f"UNPROFITABLE {vid}: {p} LKR")

    print(f"{date}: {n} vehicles, fleet profit {profit} LKR, {losers} unprofitable")


default_args = {
    "owner": "fleet-ops",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_reconciliation",
    description="Reconcile one simulated day of telemetry against garage expense files",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=12),      # 1 simulated day = 24 real minutes
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    user_defined_macros={"sim_yesterday": sim_yesterday},
    tags=["lambda", "batch-layer"],
) as dag:

    wait_for_expenses = FileSensor(
        task_id="wait_for_expense_file",
        filepath="/opt/app/data/landing/expenses_{{ sim_yesterday() }}.csv",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=60 * 10,
        mode="reschedule",      # frees the worker slot between pokes
        soft_fail=True,         # no file yet for this simulated day -> skip the run
    )

    validate = PythonOperator(
        task_id="validate_expense_file",
        python_callable=validate_expenses,
        op_kwargs={"date": "{{ sim_yesterday() }}"},
    )

    run_batch_job = BashOperator(
        task_id="spark_profitability_job",
        bash_command=(
            "spark-submit --master 'local[2]' "
            "--packages org.postgresql:postgresql:42.7.3 "
            "--conf spark.jars.ivy=/home/airflow/.ivy2 "
            "--conf spark.sql.shuffle.partitions=4 "
            "/opt/app/batch/profitability_job.py --date {{ sim_yesterday() }}"
        ),
    )

    check = PythonOperator(
        task_id="check_results",
        python_callable=check_results,
        op_kwargs={"date": "{{ sim_yesterday() }}"},
    )

    wait_for_expenses >> validate >> run_batch_job >> check