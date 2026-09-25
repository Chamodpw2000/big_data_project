"""Shared configuration for the fleet Lambda pipeline.

Every tunable number lives here so the simulators, Spark jobs and API all
agree on the same simulated world.
"""
import os
from datetime import datetime

# ---------- Connections ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_TELEMETRY = "fleet.telemetry"
PG_DSN = os.getenv("PG_DSN", "postgresql://fleet:fleet@localhost:5432/fleet")

# ---------- Paths ----------
LANDING_DIR = os.getenv("LANDING_DIR", "data/landing")
LAKE_DIR = os.getenv("LAKE_DIR", "data/lake")
REPORT_DIR = os.getenv("REPORT_DIR", "data/reports")

# ---------- Simulated clock ----------
# 1 simulated day = 24 real minutes  ->  1 sim hour = 1 real minute
SIM_DAY_REAL_MINUTES = 24
SIM_COMPRESSION = (24 * 60) / SIM_DAY_REAL_MINUTES      # = 60 sim seconds per real second
SIM_START = datetime(2026, 1, 1, 0, 0, 0)               # sim day 1 starts here

# ---------- Event generation ----------
EMIT_INTERVAL_SEC = 2.5          # real seconds between telemetry rounds
LATE_EVENT_PROB = 0.05           # 5% of events arrive late (tests the watermark)
LATE_EVENT_MAX_SIM_MIN = 8

# ---------- Money (LKR) ----------
CURRENCY = "LKR"
BASE_FARE = 150.0                # flag-down fare
FARE_PER_KM = 90.0
FUEL_COST_PER_KM = 28.0
MAINTENANCE_PER_KM = 6.0
SERVICE_COST_RANGE = (6000.0, 15000.0)   # when service_flag is true
SERVICE_PROBABILITY = 0.10               # chance a vehicle is serviced on a given day

# ---------- Driving behaviour ----------
SPEED_ON_TRIP = (20.0, 55.0)     # km/h
SPEED_ENROUTE = (15.0, 45.0)
IDLE_SPEED = (0.0, 3.0)

# ---------- Fleet: 20 vehicles with three behaviour profiles ----------
# "trip_chance" = probability an idle vehicle starts a new trip each round.
# Tuned so a simulated day gives realistic volumes:
#   busy ~25 trips/day, normal ~15, idle_prone ~5 (cruising empty -> unprofitable)
PROFILES = {
    "busy":       {"trip_chance": 0.060, "daily_km": (150, 200)},
    "normal":     {"trip_chance": 0.030, "daily_km": (100, 150)},
    "idle_prone": {"trip_chance": 0.009, "daily_km": (90, 140)},
}

_HOME_ZONES = ["Fort", "Kollupitiya", "Borella", "Dehiwala"]


def _profile_for(i: int) -> str:
    if i <= 3:
        return "busy"
    if i >= 18:
        return "idle_prone"
    return "normal"


FLEET = [
    {
        "vehicle_id": f"V{i:02d}",
        "driver_id": f"D{i:02d}",
        "profile": _profile_for(i),
        "home_zone": _HOME_ZONES[(i - 1) % len(_HOME_ZONES)],
    }
    for i in range(1, 21)
]


# ---------- Peak hours (simulated time) ----------
# Trip chance is multiplied by this factor, so "earnings by time-of-day" shows a pattern.
def demand_factor(sim_hour: int) -> float:
    if 7 <= sim_hour <= 9:       # morning peak
        return 1.8
    if 17 <= sim_hour <= 20:     # evening peak
        return 2.0
    if 0 <= sim_hour <= 4:       # night
        return 0.3
    return 1.0