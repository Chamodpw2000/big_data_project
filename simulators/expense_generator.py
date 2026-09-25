"""Daily-batch source: drops one CSV of vehicle expenses per simulated day.

Columns follow the brief exactly:
    vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag

Run:
    python -m simulators.expense_generator --backfill 3     # write 3 past days at once
    python -m simulators.expense_generator                  # watch the clock, write each day
    python -m simulators.expense_generator --date 2026-01-02
"""
import argparse
import csv
import os
import random
import time
from datetime import timedelta

from common.config import (FLEET, FUEL_COST_PER_KM, LANDING_DIR,
                           MAINTENANCE_PER_KM, PROFILES, SERVICE_COST_RANGE,
                           SERVICE_PROBABILITY)
from common.logging_config import get_logger, log_event
from common.sim_clock import SimClock

log = get_logger("expense_generator")
HEADER = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]


def day_rows(date_str: str):
    """One expense row per vehicle for the given simulated day."""
    rows = []
    for spec in FLEET:
        lo, hi = PROFILES[spec["profile"]]["daily_km"]
        distance = round(random.uniform(lo, hi), 1)
        fuel = round(distance * FUEL_COST_PER_KM * random.uniform(0.9, 1.15), 2)
        maintenance = round(distance * MAINTENANCE_PER_KM * random.uniform(0.8, 1.2), 2)
        service = random.random() < SERVICE_PROBABILITY
        if service:
            maintenance = round(maintenance + random.uniform(*SERVICE_COST_RANGE), 2)
        rows.append({
            "vehicle_id": spec["vehicle_id"],
            "fuel_cost": fuel,
            "maintenance_cost": maintenance,
            "distance_covered": distance,
            "service_flag": str(service).lower(),
        })
    return rows


def write_day(date_str: str, landing_dir: str = LANDING_DIR) -> str:
    os.makedirs(landing_dir, exist_ok=True)
    path = os.path.join(landing_dir, f"expenses_{date_str}.csv")
    tmp = path + ".tmp"                      # write then rename, so the FileSensor
    rows = day_rows(date_str)                # never sees a half-written file
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)
    log_event(log, "expense file written", file=path, rows=len(rows), report_date=date_str)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="write one file for this simulated date (YYYY-MM-DD)")
    ap.add_argument("--backfill", type=int, default=0,
                    help="write N past simulated days ending yesterday, then exit")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    clock = SimClock()

    if args.date:
        write_day(args.date)
        return

    if args.backfill:
        today = clock.now().date()
        for i in range(args.backfill, 0, -1):
            write_day((today - timedelta(days=i)).strftime("%Y-%m-%d"))
        return

    # Watch mode: each time the simulated date rolls over, publish the finished day.
    log_event(log, "watching simulated clock", landing_dir=LANDING_DIR)
    current = clock.now().date()
    try:
        while True:
            time.sleep(5)
            now = clock.now().date()
            if now != current:
                write_day(current.strftime("%Y-%m-%d"))
                current = now
    except KeyboardInterrupt:
        log_event(log, "expense generator stopped")


if __name__ == "__main__":
    main()