"""SERVING LAYER - live operations dashboard (Streamlit).

Reads only from the FastAPI service, never from the database directly, so the
API stays the single serving interface. Polls every few seconds.

    streamlit run serving/dashboard/app.py
"""
import os
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))

st.set_page_config(page_title="Fleet operations", page_icon="", layout="wide")


def api(path: str):
    try:
        r = requests.get(f"{API_URL}{path}", timeout=5)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        st.error(f"API unreachable at {API_URL}{path}: {e}")
        return None


st.title("Ride-hailing fleet operations")
st.caption("Lambda architecture - speed layer (live) and batch layer (daily reconciliation)")

health = api("/health")
metrics = api("/metrics/realtime")

# ---------------- component health ----------------
if health:
    cols = st.columns(len(health["components"]) + 1 or 1)
    badge = "OK" if health["status"] == "healthy" else "DEGRADED"
    cols[0].metric("Pipeline", badge)
    for c, comp in zip(cols[1:], health["components"]):
        c.metric(comp["component"], comp["status"], f"{comp['seconds_since']}s ago")

st.divider()

# ---------------- speed layer: right now ----------------
if metrics:
    f = metrics["fleet"]
    st.subheader(f"Right now - simulated time {metrics['sim_time']}")
    k = st.columns(5)
    k[0].metric("Fleet size", f["fleet_size"])
    k[1].metric("Active vehicles", f["active_vehicles"])
    k[2].metric("On trip", f["on_trip"])
    k[3].metric("Idle ratio", f"{(f['idle_ratio'] or 0):.0%}")
    k[4].metric("Earnings (last hour)", f"{metrics['total_earnings_latest_hour']:,.0f} {metrics['currency']}")

    zones = pd.DataFrame(metrics["zones"])
    if not zones.empty:
        left, right = st.columns(2)
        with left:
            st.caption("Earnings by zone - latest completed hour")
            st.bar_chart(zones.set_index("zone")["earnings"])
        with right:
            st.caption("Trips per hour by zone")
            st.bar_chart(zones.set_index("zone")["trips_per_hour"])
        st.dataframe(
            zones[["zone", "window_start", "active_vehicles", "trips_per_hour",
                   "idle_ratio", "earnings"]],
            use_container_width=True, hide_index=True)
    else:
        st.info("No completed windows yet - wait for the first simulated hour.")

st.divider()

# ---------------- time of day ----------------
windows = api("/metrics/zones?limit=96")
if windows and windows["windows"]:
    w = pd.DataFrame(windows["windows"])
    w["window_start"] = pd.to_datetime(w["window_start"])
    pivot = w.pivot_table(index="window_start", columns="zone",
                          values="earnings", aggfunc="sum").fillna(0)
    st.subheader("Earnings by area and time of day")
    st.line_chart(pivot)

st.divider()

# ---------------- alerts and vehicles ----------------
left, right = st.columns([1, 1])

with left:
    st.subheader("Open alerts")
    alerts = api("/alerts?open_only=true&limit=25")
    if alerts and alerts["alerts"]:
        a = pd.DataFrame(alerts["alerts"])
        st.dataframe(a[["alert_type", "severity", "vehicle_id", "zone", "message"]],
                     use_container_width=True, hide_index=True)
    else:
        st.success("No open alerts")

with right:
    st.subheader("Vehicles")
    vehicles = api("/vehicles")
    if vehicles:
        v = pd.DataFrame(vehicles["vehicles"])
        if not v.empty:
            st.dataframe(v[["vehicle_id", "status", "zone", "idle_minutes", "last_event_ts"]],
                         use_container_width=True, hide_index=True, height=320)

st.divider()

# ---------------- batch layer ----------------
st.subheader("Daily profitability (batch layer)")
days = api("/profitability")
if days and days["days"]:
    options = [str(d["report_date"]) for d in days["days"]]
    chosen = st.selectbox("Simulated day", options)
    report = api(f"/profitability/{chosen}")
    if report:
        c = st.columns(4)
        c[0].metric("Fleet earnings", f"{report['fleet_earnings']:,.0f}")
        c[1].metric("Fleet profit", f"{report['fleet_profit']:,.0f}")
        c[2].metric("Unprofitable vehicles", report["unprofitable_count"])
        c[3].metric("Vehicles", len(report["vehicles"]))

        d = pd.DataFrame(report["vehicles"])
        st.bar_chart(d.set_index("vehicle_id")["profit"])
        st.dataframe(
            d[["vehicle_id", "trips", "earnings", "total_cost", "profit",
               "profit_margin", "service_flag", "is_unprofitable"]],
            use_container_width=True, hide_index=True)
else:
    st.info("No reconciled days yet - the Airflow DAG writes these once a simulated day ends.")

st.caption(f"Polling {API_URL} every {REFRESH_SECONDS}s - last update "
           f"{datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.checkbox("Auto-refresh", value=True):
    time.sleep(REFRESH_SECONDS)
    st.rerun()