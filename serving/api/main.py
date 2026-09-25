"""SERVING LAYER - FastAPI.

Reads only from Postgres, which both Lambda layers write to, and exposes:

  /health                      component liveness (observability requirement)
  /metrics/realtime            speed layer: active vehicles, idle ratio, trips/hour, earnings by zone
  /metrics/zones               recent 1-hour windows per zone (time-of-day pattern)
  /vehicles                    latest state of every vehicle
  /alerts                      threshold alerts (long idle, unprofitable)
  /profitability/{date}        batch layer: per-vehicle profit for one simulated day
  /profitability/{date}/unprofitable

The API never touches Kafka or the lake: the serving layer only queries
pre-computed views, which is what keeps it fast.
"""
import os
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query

PG_DSN = os.getenv("PG_DSN", "postgresql://fleet:fleet@postgres:5432/fleet")
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "60"))
CURRENCY = "LKR"

app = FastAPI(
    title="Fleet Operations API",
    description="Serving layer of a Lambda architecture: real-time fleet metrics "
                "from the speed layer and daily profitability from the batch layer.",
    version="1.0.0",
)


def query(sql: str, params: tuple = ()) -> list:
    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=503, detail=f"database unavailable: {e}")


def as_float(row: dict, *keys):
    for k in keys:
        if row.get(k) is not None:
            row[k] = float(row[k])
    return row


# ---------------------------------------------------------------- health
@app.get("/health", tags=["observability"])
def health():
    """Liveness of every pipeline component, based on its heartbeat."""
    rows = query("""
        SELECT component, last_seen, status, records_last, details,
               EXTRACT(EPOCH FROM (NOW() - last_seen)) AS age_seconds
        FROM pipeline_heartbeat ORDER BY component
    """)
    components = []
    healthy = True
    for r in rows:
        stale = float(r["age_seconds"]) > STALE_AFTER_SECONDS
        if stale or r["status"] != "OK":
            healthy = False
        components.append({
            "component": r["component"],
            "status": "STALE" if stale else r["status"],
            "last_seen": r["last_seen"],
            "seconds_since": round(float(r["age_seconds"]), 1),
            "records_last": r["records_last"],
            "details": r["details"],
        })
    if not rows:
        healthy = False
    return {
        "status": "healthy" if healthy else "degraded",
        "checked_at": datetime.utcnow(),
        "stale_after_seconds": STALE_AFTER_SECONDS,
        "components": components,
    }


# ------------------------------------------------------- speed layer views
@app.get("/metrics/realtime", tags=["speed layer"])
def realtime_metrics():
    """Fleet utilization right now, plus the latest completed hour per zone."""
    fleet = query("""
        SELECT COUNT(*) AS fleet_size,
               COUNT(*) FILTER (WHERE status = 'on_trip') AS on_trip,
               COUNT(*) FILTER (WHERE status = 'enroute') AS enroute,
               COUNT(*) FILTER (WHERE status = 'idle')    AS idle,
               MAX(last_event_ts) AS sim_time
        FROM vehicle_status
    """)[0]

    size = fleet["fleet_size"] or 0
    busy = (fleet["on_trip"] or 0) + (fleet["enroute"] or 0)
    fleet["active_vehicles"] = busy
    fleet["idle_ratio"] = round(
        (fleet["idle"] or 0) / size, 4) if size else None
    fleet["utilization"] = round(busy / size, 4) if size else None

    zones = query("""
        SELECT DISTINCT ON (zone)
               zone, window_start, window_end, active_vehicles,
               trips_completed AS trips_per_hour, idle_ratio, earnings
        FROM realtime_zone_metrics
        ORDER BY zone, window_start DESC
    """)
    zones = [as_float(z, "idle_ratio", "earnings") for z in zones]

    return {
        "currency": CURRENCY,
        "sim_time": fleet.pop("sim_time"),
        "fleet": fleet,
        "zones": zones,
        "total_earnings_latest_hour": round(sum(z["earnings"] for z in zones), 2) if zones else 0.0,
    }


@app.get("/metrics/zones", tags=["speed layer"])
def zone_windows(limit: int = Query(24, ge=1, le=200), zone: Optional[str] = None):
    """Recent hourly windows - the 'earnings by area / time-of-day' view."""
    if zone:
        rows = query("""
            SELECT window_start, window_end, zone, active_vehicles, on_trip_events,
                   idle_events, enroute_events, idle_ratio, trips_completed, earnings
            FROM realtime_zone_metrics WHERE zone = %s
            ORDER BY window_start DESC LIMIT %s
        """, (zone, limit))
    else:
        rows = query("""
            SELECT window_start, window_end, zone, active_vehicles, on_trip_events,
                   idle_events, enroute_events, idle_ratio, trips_completed, earnings
            FROM realtime_zone_metrics
            ORDER BY window_start DESC, zone LIMIT %s
        """, (limit,))
    return {"currency": CURRENCY, "windows": [as_float(r, "idle_ratio", "earnings") for r in rows]}


@app.get("/vehicles", tags=["speed layer"])
def vehicles(status: Optional[str] = None):
    """Latest known state of each vehicle, with how long it has been idle."""
    sql = """
        SELECT vehicle_id, driver_id, zone, status, lat, lon, idle_since, last_event_ts,
               CASE WHEN idle_since IS NOT NULL
                    THEN ROUND(EXTRACT(EPOCH FROM (last_event_ts - idle_since)) / 60)
               END AS idle_minutes
        FROM vehicle_status
    """
    params = ()
    if status:
        sql += " WHERE status = %s"
        params = (status,)
    sql += " ORDER BY vehicle_id"
    return {"vehicles": [as_float(r, "lat", "lon", "idle_minutes") for r in query(sql, params)]}


@app.get("/alerts", tags=["alerts"])
def alerts(open_only: bool = True, limit: int = Query(50, ge=1, le=500)):
    sql = """
        SELECT alert_id, alert_type, severity, vehicle_id, zone, message,
               sim_time, created_at, resolved
        FROM alerts
    """
    if open_only:
        sql += " WHERE resolved = FALSE"
    sql += " ORDER BY alert_id DESC LIMIT %s"
    return {"alerts": query(sql, (limit,))}


# ------------------------------------------------------- batch layer views
@app.get("/profitability/{report_date}", tags=["batch layer"])
def profitability(report_date: str):
    rows = query("""
        SELECT vehicle_id, trips, earnings, distance_covered, fuel_cost, maintenance_cost,
               total_cost, profit, profit_margin, service_flag, is_unprofitable, computed_at
        FROM daily_vehicle_profitability WHERE report_date = %s ORDER BY profit
    """, (report_date,))
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"no reconciliation for {report_date}")
    rows = [as_float(r, "earnings", "distance_covered", "fuel_cost", "maintenance_cost",
                     "total_cost", "profit", "profit_margin") for r in rows]
    return {
        "report_date": report_date,
        "currency": CURRENCY,
        "fleet_profit": round(sum(r["profit"] for r in rows), 2),
        "fleet_earnings": round(sum(r["earnings"] for r in rows), 2),
        "unprofitable_count": sum(1 for r in rows if r["is_unprofitable"]),
        "vehicles": rows,
    }


@app.get("/profitability/{report_date}/unprofitable", tags=["batch layer"])
def unprofitable(report_date: str):
    rows = query("""
        SELECT vehicle_id, trips, earnings, total_cost, profit, profit_margin, service_flag
        FROM daily_vehicle_profitability
        WHERE report_date = %s AND is_unprofitable ORDER BY profit
    """, (report_date,))
    return {
        "report_date": report_date,
        "currency": CURRENCY,
        "count": len(rows),
        "vehicles": [as_float(r, "earnings", "total_cost", "profit", "profit_margin") for r in rows],
    }


@app.get("/profitability", tags=["batch layer"])
def available_days():
    """Which simulated days the batch layer has reconciled."""
    return {"days": query("""
        SELECT report_date, COUNT(*) AS vehicles,
               SUM(profit) AS fleet_profit,
               COUNT(*) FILTER (WHERE is_unprofitable) AS unprofitable
        FROM daily_vehicle_profitability
        GROUP BY report_date ORDER BY report_date DESC
    """)}


@app.get("/", include_in_schema=False)
def root():
    return {"service": "fleet-operations-api", "docs": "/docs", "health": "/health"}
