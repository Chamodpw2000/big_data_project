"""SPEED LAYER - Spark Structured Streaming.

Reads telemetry from Kafka and produces two things:

  1. The master dataset  : every raw event appended to the Parquet lake.
  2. Fast, approximate views for the serving layer:
       - realtime_zone_metrics : 1-hour tumbling windows per zone (event time)
       - vehicle_status        : latest known state of each vehicle
       - alerts                : LONG_IDLE when a vehicle idles too long

All timestamps are SIMULATED time carried inside the event itself, so the job
uses event-time windows with a watermark rather than processing time.

Submitted by the "spark-speed" service in docker-compose.
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType)

from common.zones import ZONES

# ---------- Settings (inside the container these point at service names) ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_TELEMETRY", "fleet.telemetry")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_DATA_LOSS = os.getenv("FAIL_ON_DATA_LOSS", "false")
LAKE_PATH = os.getenv("LAKE_PATH", "data/lake/raw/telemetry")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/checkpoints")

JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/fleet")
PG_USER = os.getenv("PG_USER", "fleet")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fleet")
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

WATERMARK = os.getenv("WATERMARK", "10 minutes")      # simulated minutes
WINDOW = os.getenv("WINDOW", "1 hour")                # simulated hour = 1 real minute
IDLE_ALERT_MINUTES = int(os.getenv("IDLE_ALERT_MINUTES", "60"))   # simulated minutes
TRIGGER = os.getenv("TRIGGER", "10 seconds")

EVENT_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("status", StringType()),
    StructField("fare", DoubleType()),
    StructField("timestamp", StringType()),
])


# ---------- Helpers ----------
def zone_expression(lat_col, lon_col):
    """lat/lon -> zone name, as pure Spark SQL (no Python UDF, so it stays fast)."""
    expr = F.lit("Unknown")
    for z in reversed(ZONES):
        inside = (lat_col.between(z["lat"][0], z["lat"][1]) &
                  lon_col.between(z["lon"][0], z["lon"][1]))
        expr = F.when(inside, F.lit(z["name"])).otherwise(expr)
    return expr


def execute_sql(spark, sql: str):
    """Run a statement on Postgres through the JDBC driver already on the classpath."""
    conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)
    try:
        st = conn.createStatement()
        st.execute(sql)
        st.close()
    finally:
        conn.close()


def heartbeat(spark, component: str, records: int, details: str):
    execute_sql(spark, f"""
        INSERT INTO pipeline_heartbeat (component, last_seen, status, records_last, details)
        VALUES ('{component}', NOW(), 'OK', {records}, '{details}')
        ON CONFLICT (component) DO UPDATE SET
            last_seen = NOW(), status = 'OK',
            records_last = EXCLUDED.records_last, details = EXCLUDED.details;
    """)


# ---------- Sink 1: raw events -> lake, vehicle status, alerts ----------
def process_raw_batch(batch_df, epoch_id):
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    try:
        count = batch_df.count()
        if count == 0:
            return

        # (a) master dataset: append raw events, partitioned by simulated date
        (batch_df
            .withColumn("event_date", F.to_date("event_time"))
            .write.mode("append").partitionBy("event_date").parquet(LAKE_PATH))

        # (b) latest row per vehicle in this micro-batch
        newest = Window.partitionBy("vehicle_id").orderBy(F.col("event_time").desc())
        latest = (batch_df
                  .withColumn("_rn", F.row_number().over(newest))
                  .filter(F.col("_rn") == 1)
                  .select("vehicle_id", "driver_id", "zone", "status", "lat", "lon",
                          F.col("event_time").alias("last_event_ts")))
        latest.write.jdbc(JDBC_URL, "stg_vehicle_status", mode="overwrite", properties=PG_PROPS)

        # idle_since survives across batches: only reset when the vehicle stops being idle.
        # The WHERE clause drops late events that are older than what we already stored.
        execute_sql(spark, """
            INSERT INTO vehicle_status
                (vehicle_id, driver_id, zone, status, lat, lon, idle_since, last_event_ts, updated_at)
            SELECT vehicle_id, driver_id, zone, status, lat, lon,
                   CASE WHEN status = 'idle' THEN last_event_ts END,
                   last_event_ts, NOW()
            FROM stg_vehicle_status
            ON CONFLICT (vehicle_id) DO UPDATE SET
                driver_id = EXCLUDED.driver_id,
                zone      = EXCLUDED.zone,
                status    = EXCLUDED.status,
                lat       = EXCLUDED.lat,
                lon       = EXCLUDED.lon,
                idle_since = CASE
                    WHEN EXCLUDED.status <> 'idle' THEN NULL
                    WHEN vehicle_status.status = 'idle' AND vehicle_status.idle_since IS NOT NULL
                        THEN vehicle_status.idle_since
                    ELSE EXCLUDED.last_event_ts END,
                last_event_ts = EXCLUDED.last_event_ts,
                updated_at = NOW()
            WHERE EXCLUDED.last_event_ts >= vehicle_status.last_event_ts;
        """)

        # (c) threshold alert: idle longer than IDLE_ALERT_MINUTES simulated minutes.
        # NOT EXISTS keeps one open alert per vehicle instead of one per micro-batch.
        execute_sql(spark, f"""
            INSERT INTO alerts (alert_type, severity, vehicle_id, zone, message, sim_time)
            SELECT 'LONG_IDLE', 'WARNING', v.vehicle_id, v.zone,
                   'Idle for ' ||
                   ROUND(EXTRACT(EPOCH FROM (v.last_event_ts - v.idle_since)) / 60) ||
                   ' simulated minutes',
                   v.last_event_ts
            FROM vehicle_status v
            WHERE v.status = 'idle'
              AND v.idle_since IS NOT NULL
              AND v.last_event_ts - v.idle_since >= INTERVAL '{IDLE_ALERT_MINUTES} minutes'
              AND NOT EXISTS (
                    SELECT 1 FROM alerts a
                    WHERE a.vehicle_id = v.vehicle_id
                      AND a.alert_type = 'LONG_IDLE'
                      AND a.resolved = FALSE);
        """)

        # close alerts for vehicles that started moving again
        execute_sql(spark, """
            UPDATE alerts a SET resolved = TRUE
            FROM vehicle_status v
            WHERE a.vehicle_id = v.vehicle_id
              AND a.alert_type = 'LONG_IDLE'
              AND a.resolved = FALSE
              AND v.status <> 'idle';
        """)

        heartbeat(spark, "speed_layer_raw", count, f"batch {epoch_id}")
    finally:
        batch_df.unpersist()


# ---------- Sink 2: windowed zone metrics ----------
def upsert_metrics_batch(batch_df, epoch_id):
    spark = batch_df.sparkSession
    if batch_df.isEmpty():
        return
    batch_df.write.jdbc(JDBC_URL, "stg_zone_metrics", mode="overwrite", properties=PG_PROPS)
    execute_sql(spark, """
        INSERT INTO realtime_zone_metrics
            (window_start, window_end, zone, active_vehicles, on_trip_events, idle_events,
             enroute_events, idle_ratio, trips_completed, earnings, updated_at)
        SELECT window_start, window_end, zone, active_vehicles, on_trip_events, idle_events,
               enroute_events, idle_ratio, trips_completed, earnings, NOW()
        FROM stg_zone_metrics
        ON CONFLICT (window_start, zone) DO UPDATE SET
            window_end      = EXCLUDED.window_end,
            active_vehicles = EXCLUDED.active_vehicles,
            on_trip_events  = EXCLUDED.on_trip_events,
            idle_events     = EXCLUDED.idle_events,
            enroute_events  = EXCLUDED.enroute_events,
            idle_ratio      = EXCLUDED.idle_ratio,
            trips_completed = EXCLUDED.trips_completed,
            earnings        = EXCLUDED.earnings,
            updated_at      = NOW();
    """)
    heartbeat(spark, "speed_layer_metrics", batch_df.count(), f"batch {epoch_id}")


def main():
    spark = (SparkSession.builder
             .appName("fleet-speed-layer")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.schemaInference", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", STARTING_OFFSETS)
           # Kafka is not persisted in this dev setup, so a "docker compose down"
           # resets its offsets while the Spark checkpoint still remembers the old
           # ones. In production this would stay true (data loss must be visible);
           # here we skip to whatever offsets Kafka currently has.
           .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
           .load())

    events = (raw
              .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.to_timestamp("timestamp"))
              .withColumn("zone", zone_expression(F.col("lat"), F.col("lon")))
              .drop("timestamp")
              .filter(F.col("event_time").isNotNull()))

    q_raw = (events.writeStream
             .foreachBatch(process_raw_batch)
             .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
             .trigger(processingTime=TRIGGER)
             .start())

    total_events = (F.col("on_trip_events") + F.col("idle_events") + F.col("enroute_events"))
    metrics = (events
               .withWatermark("event_time", WATERMARK)
               .groupBy(F.window("event_time", WINDOW), F.col("zone"))
               .agg(
                   F.approx_count_distinct("vehicle_id").alias("active_vehicles"),
                   F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("on_trip_events"),
                   F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("idle_events"),
                   F.sum(F.when(F.col("status") == "enroute", 1).otherwise(0)).alias("enroute_events"),
                   F.approx_count_distinct(
                       F.when(F.col("fare").isNotNull(), F.col("trip_id"))).alias("trips_completed"),
                   F.sum(F.coalesce(F.col("fare"), F.lit(0.0))).alias("earnings"))
               .select(
                   F.col("window.start").alias("window_start"),
                   F.col("window.end").alias("window_end"),
                   F.col("zone"),
                   F.col("active_vehicles"),
                   F.col("on_trip_events"), F.col("idle_events"), F.col("enroute_events"),
                   F.round(F.col("idle_events") / F.greatest(total_events, F.lit(1)), 4).alias("idle_ratio"),
                   F.col("trips_completed"),
                   F.round(F.col("earnings"), 2).alias("earnings")))

    q_metrics = (metrics.writeStream
                 .outputMode("update")          # emit windows whose values changed
                 .foreachBatch(upsert_metrics_batch)
                 .option("checkpointLocation", f"{CHECKPOINT_ROOT}/metrics")
                 .trigger(processingTime=TRIGGER)
                 .start())

    print(f"[speed_layer] reading {TOPIC} from {KAFKA_BOOTSTRAP}; "
          f"window={WINDOW} watermark={WATERMARK} idle_alert={IDLE_ALERT_MINUTES}m", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()