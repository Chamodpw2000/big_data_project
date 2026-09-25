-- ============================================================
-- Fleet Lambda Pipeline - serving schema (database: fleet)
-- All times are SIMULATED time (1 sim day = 24 real minutes).
-- ============================================================

-- ---------- SPEED LAYER outputs (written by Spark Structured Streaming) ----------

-- 1-hour tumbling-window metrics per zone -> "utilization & earnings by area/time-of-day"
CREATE TABLE IF NOT EXISTS realtime_zone_metrics (
    window_start     TIMESTAMP     NOT NULL,
    window_end       TIMESTAMP     NOT NULL,
    zone             VARCHAR(20)   NOT NULL,
    active_vehicles  INT           NOT NULL,   -- distinct vehicles seen in window
    on_trip_events   INT           NOT NULL,
    idle_events      INT           NOT NULL,
    enroute_events   INT           NOT NULL,
    idle_ratio       NUMERIC(5,4)  NOT NULL,   -- idle_events / total events
    trips_completed  INT           NOT NULL,   -- distinct trip_ids with a fare
    earnings         NUMERIC(12,2) NOT NULL,
    updated_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, zone)           -- upsert target (window updates as late data arrives)
);

-- Latest known state of each vehicle -> "right now" view + idle detection
CREATE TABLE IF NOT EXISTS vehicle_status (
    vehicle_id    VARCHAR(10)  PRIMARY KEY,
    driver_id     VARCHAR(10),
    zone          VARCHAR(20),
    status        VARCHAR(10)  CHECK (status IN ('idle','enroute','on_trip')),
    lat           NUMERIC(9,6),
    lon           NUMERIC(9,6),
    idle_since    TIMESTAMP,                   -- NULL when not idle
    last_event_ts TIMESTAMP    NOT NULL,       -- event time of last reading
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Alerts raised by speed layer (long idle) and health checks (no data)
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    SERIAL       PRIMARY KEY,
    alert_type  VARCHAR(30)  NOT NULL,         -- LONG_IDLE | NO_DATA | BATCH_FAILED ...
    severity    VARCHAR(10)  NOT NULL CHECK (severity IN ('INFO','WARNING','CRITICAL')),
    vehicle_id  VARCHAR(10),
    zone        VARCHAR(20),
    message     TEXT         NOT NULL,
    sim_time    TIMESTAMP,                     -- simulated time the condition occurred
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    resolved    BOOLEAN      NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alerts_open ON alerts (resolved, created_at DESC);

-- ---------- BATCH LAYER output (written by Spark batch job via Airflow) ----------

-- Accurate per-vehicle profitability for one simulated day
CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    report_date       DATE          NOT NULL,
    vehicle_id        VARCHAR(10)   NOT NULL,
    trips             INT           NOT NULL,
    earnings          NUMERIC(12,2) NOT NULL,  -- from raw telemetry in Parquet lake
    distance_covered  NUMERIC(10,2),           -- from expense file
    fuel_cost         NUMERIC(12,2) NOT NULL,
    maintenance_cost  NUMERIC(12,2) NOT NULL,
    total_cost        NUMERIC(12,2) NOT NULL,
    profit            NUMERIC(12,2) NOT NULL,  -- earnings - total_cost
    profit_margin     NUMERIC(6,4),            -- profit / earnings
    service_flag      BOOLEAN       NOT NULL DEFAULT FALSE,
    is_unprofitable   BOOLEAN       NOT NULL,
    computed_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, vehicle_id)      -- re-running a day overwrites it (idempotent)
);

-- ---------- OBSERVABILITY ----------

-- Each component writes a heartbeat; the health check flags stale components
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component     VARCHAR(40)  PRIMARY KEY,    -- gps_simulator | speed_layer | batch_job ...
    last_seen     TIMESTAMP    NOT NULL,
    status        VARCHAR(10)  NOT NULL,       -- OK | ERROR
    records_last  INT,                         -- records handled in last cycle/batch
    details       TEXT
);