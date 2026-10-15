"""Databricks Delta Live Tables (Lakeflow Declarative Pipelines): Bronze -> Silver -> Gold.   *** NOT YET RUN ***
Create a DLT pipeline on this file and set pipeline configuration `landing_base` = s3://<bucket>/landing with
sub-folders  trips/ (Parquet from MySQL export),  pings/ (raw_stream/driver_gps_pings Parquet),  surge/ (bronze_surge Parquet).
If DLT is unavailable on the Free Edition, run the same transformations as plain Delta jobs (same predicates as .filter()).
"""
import dlt
from pyspark.sql import functions as F

BASE = spark.conf.get("landing_base")  # noqa: F821 - `spark` is injected by Databricks
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 17.10, 17.75, 78.15, 78.75        # Hyderabad bounds


def _auto_loader(folder: str):
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")  # noqa: F821
            .option("cloudFiles.schemaLocation", f"{BASE}/_schemas/{folder}").load(f"{BASE}/{folder}"))


# ---------- Bronze: as received, no rules ----------
@dlt.table(comment="Trips as exported from MySQL")
def bronze_trips(): return _auto_loader("trips")

@dlt.table(comment="Raw GPS pings from Kafka")
def bronze_pings(): return _auto_loader("pings")

@dlt.table(comment="Surge engine output")
def bronze_surge(): return _auto_loader("surge")

# ---------- Silver: cleaned, typed, deduplicated ----------
@dlt.table(comment="Valid trips")
@dlt.expect_or_drop("non_null_zone", "pickup_zone_id IS NOT NULL AND dropoff_zone_id IS NOT NULL")
@dlt.expect_or_drop("fare_not_negative", "fare IS NULL OR fare >= 0")
@dlt.expect_or_drop("pickup_after_request", "pickup_time IS NULL OR pickup_time >= request_time")
@dlt.expect_or_drop("dropoff_after_pickup", "pickup_time IS NULL OR dropoff_time IS NULL OR dropoff_time >= pickup_time")
def silver_trips():
    return dlt.read_stream("bronze_trips").dropDuplicates(["trip_id"])

@dlt.table(comment="Valid GPS pings inside Hyderabad")
@dlt.expect_or_drop("valid_gps", f"lat BETWEEN {LAT_MIN} AND {LAT_MAX} AND lon BETWEEN {LON_MIN} AND {LON_MAX}")
@dlt.expect_or_drop("non_null_zone", "zone_id IS NOT NULL")
def silver_pings():
    return dlt.read_stream("bronze_pings").withColumn("ts", F.to_timestamp("event_time")).dropDuplicates(["event_id"])

# ---------- Gold ----------
@dlt.table(comment="Surge per zone and minute")
def zone_minute_surge():
    return dlt.read("bronze_surge").select("zone_id", "window_end", "demand", "supply", "on_trip", "ratio", "surge_multiplier", "rain_mm_h", "temp_c")

@dlt.table(comment="Hourly KPIs per zone")
def zone_hourly_kpis():
    t = dlt.read("silver_trips")
    return (t.groupBy("pickup_zone_id", F.date_trunc("hour", "request_time").alias("hour_ts")).agg(
        F.count("*").alias("requests"),
        F.avg((F.unix_timestamp("pickup_time") - F.unix_timestamp("request_time")) / 60).alias("avg_eta_min"),
        F.sum("fare").alias("revenue_inr"), F.sum("distance_km").alias("km")))

@dlt.table(comment="Driver utilisation = on-trip share of GPS pings, per driver per day")
def driver_daily_utilisation():
    p = dlt.read("silver_pings").where("source = 'gps_simulator'")
    return (p.groupBy("driver_id", F.to_date("ts").alias("date")).agg(
        (F.sum((F.col("status") == "ON_TRIP").cast("int")) / F.count("*")).alias("utilisation"), F.count("*").alias("pings")))

@dlt.table(comment="Revenue per km by zone and hour")
def revenue_per_km():
    t = dlt.read("silver_trips")
    return (t.groupBy("pickup_zone_id", F.hour("request_time").alias("hour")).agg(
        (F.sum("fare") / F.greatest(F.sum("distance_km"), F.lit(0.001))).alias("revenue_per_km")))
# demand_forecast: written by ml/score.py
