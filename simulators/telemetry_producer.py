"""Streaming source: emits GPS/telemetry events to Kafka every few seconds.

Each vehicle is a small state machine:  idle -> enroute -> on_trip -> idle
The final event of a trip carries the fare, so the speed layer can count
completed trips and sum earnings.

Run:
    python -m simulators.telemetry_producer              # send to Kafka
    python -m simulators.telemetry_producer --dry-run    # print only, no Kafka
    python -m simulators.telemetry_producer --duration 60
"""
import argparse
import json
import random
import time
from datetime import timedelta

from common.config import (BASE_FARE, EMIT_INTERVAL_SEC, FARE_PER_KM, FLEET,
                           IDLE_SPEED, KAFKA_BOOTSTRAP, LATE_EVENT_MAX_SIM_MIN,
                           LATE_EVENT_PROB, PROFILES, SIM_COMPRESSION,
                           SPEED_ENROUTE, SPEED_ON_TRIP, TOPIC_TELEMETRY,
                           demand_factor)
from common.logging_config import get_logger, log_event
from common.sim_clock import SimClock
from common.zones import ZONE_NAMES, km_to_degrees, zone_center, zone_of

log = get_logger("gps_simulator")


class Vehicle:
    def __init__(self, spec):
        self.vehicle_id = spec["vehicle_id"]
        self.driver_id = spec["driver_id"]
        self.profile = PROFILES[spec["profile"]]
        self.lat, self.lon = zone_center(spec["home_zone"])
        self.status = "idle"
        self.trip_id = None
        self.trip_km = 0.0
        self.steps_left = 0
        self.trips_done = 0
        self.earnings = 0.0
        self.total_km = 0.0

    def _move(self, speed_kmh, sim_minutes):
        """Advance the vehicle along a random heading for sim_minutes."""
        km = speed_kmh * (sim_minutes / 60.0)
        self.lat += km_to_degrees(km) * random.uniform(-1, 1)
        self.lon += km_to_degrees(km) * random.uniform(-1, 1)
        # keep the fleet inside the operating area
        self.lat = min(max(self.lat, 6.8380), 6.9480)
        self.lon = min(max(self.lon, 79.8360), 79.8950)
        self.total_km += km
        return km

    def step(self, sim_now, sim_minutes):
        """Advance one round and return the telemetry event to publish."""
        fare = None

        if self.status == "idle":
            chance = self.profile["trip_chance"] * demand_factor(sim_now.hour)
            speed = random.uniform(*IDLE_SPEED)
            self._move(speed, sim_minutes)
            if random.random() < chance:
                self.status = "enroute"
                self.trip_id = f"T{self.vehicle_id}-{self.trips_done + 1:04d}"
                self.steps_left = random.randint(1, 3)
                self.trip_km = 0.0
            speed_out = speed

        elif self.status == "enroute":
            speed_out = random.uniform(*SPEED_ENROUTE)
            self._move(speed_out, sim_minutes)
            self.steps_left -= 1
            if self.steps_left <= 0:                 # passenger picked up
                self.status = "on_trip"
                self.steps_left = random.randint(2, 6)

        else:  # on_trip
            speed_out = random.uniform(*SPEED_ON_TRIP)
            self.trip_km += self._move(speed_out, sim_minutes)
            self.steps_left -= 1
            if self.steps_left <= 0:                 # trip ends on this event
                fare = round(BASE_FARE + FARE_PER_KM * self.trip_km, 2)
                self.trips_done += 1
                self.earnings += fare

        event = {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": round(speed_out, 1),
            "status": self.status,
            "fare": fare,                            # only set on the last event of a trip
            "timestamp": sim_now.isoformat(timespec="seconds"),
        }

        if fare is not None:                         # trip finished, vehicle frees up
            self.status = "idle"
            self.trip_id = None
        return event


def build_producer(dry_run: bool):
    if dry_run:
        return None
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    ap.add_argument("--duration", type=float, default=0, help="stop after N real seconds (0 = forever)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    clock = SimClock()
    vehicles = [Vehicle(spec) for spec in FLEET]
    producer = build_producer(args.dry_run)
    # 1 real second = SIM_COMPRESSION sim seconds = 1 sim minute at our settings
    sim_minutes_per_round = EMIT_INTERVAL_SEC * SIM_COMPRESSION / 60.0

    log_event(log, "simulator started", vehicles=len(vehicles), zones=ZONE_NAMES,
              topic=TOPIC_TELEMETRY, dry_run=args.dry_run)

    started, sent, late = time.time(), 0, 0
    try:
        while True:
            sim_now = clock.now()
            for v in vehicles:
                event = v.step(sim_now, sim_minutes_per_round)

                # a few events arrive late: their event time is older than the others
                if random.random() < LATE_EVENT_PROB:
                    delay = random.uniform(1, LATE_EVENT_MAX_SIM_MIN)
                    event["timestamp"] = (sim_now - timedelta(minutes=delay)).isoformat(timespec="seconds")
                    late += 1

                if producer:
                    producer.send(TOPIC_TELEMETRY, key=event["vehicle_id"], value=event)
                else:
                    print(json.dumps(event))
                sent += 1

            if producer:
                producer.flush()

            if sent % (len(vehicles) * 20) == 0:
                log_event(log, "heartbeat", events_sent=sent, late_events=late,
                          sim_time=sim_now.isoformat(timespec="seconds"),
                          sim_day=clock.day_index())

            if args.duration and (time.time() - started) >= args.duration:
                break
            time.sleep(EMIT_INTERVAL_SEC)
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        log_event(log, "simulator stopped", events_sent=sent, late_events=late,
                  trips_completed=sum(v.trips_done for v in vehicles))


if __name__ == "__main__":
    main()