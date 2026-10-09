"""Databricks Delta Live Tables (DLT) & Medallion Pipeline.

Medallion Architecture:
- Bronze: Raw ingested trip events and GPS pings.
- Silver: Cleaned, deduplicated, typed with quality expectations:
    @dlt.expect_or_drop("valid_gps", "lat BETWEEN 40.4 AND 41.0 AND lon BETWEEN -74.3 AND -73.6")
    @dlt.expect_or_drop("positive_fare", "fare > 0")
    @dlt.expect_or_drop("non_null_zone", "pickup_zone_id IS NOT NULL")
- Gold: Business aggregates:
    - zone_minute_surge
    - zone_hourly_kpis
    - driver_daily_utilisation
    - revenue_per_km
    - demand_forecast
"""

from __future__ import annotations

import logging
from typing import Any

try:
    import dlt  # type: ignore
    from pyspark.sql import functions as F

    @dlt.table(comment="Raw trip requests bronze table")
    def bronze_trips():
        return dlt.read_stream("raw_trip_requests")

    @dlt.table(comment="Cleaned and validated silver trips table")
    @dlt.expect_or_drop("valid_gps", "lat BETWEEN 40.4 AND 41.0 AND lon BETWEEN -74.3 AND -73.6")
    @dlt.expect_or_drop("positive_fare", "fare > 0")
    @dlt.expect_or_drop("non_null_zone", "pickup_zone_id IS NOT NULL")
    @dlt.expect_or_drop("pickup_after_request", "pickup_time >= request_time")
    def silver_trips():
        return dlt.read_stream("bronze_trips").dropDuplicates(["event_id"])

    @dlt.table(comment="Gold zone hourly KPIs")
    def zone_hourly_kpis():
        return (
            dlt.read("silver_trips")
            .groupBy(F.window("pickup_time", "1 hour"), "pickup_zone_id")
            .agg(
                F.count("*").alias("total_trips"),
                F.avg("fare").alias("avg_fare"),
                F.sum("distance_km").alias("total_distance_km"),
                F.avg("wait_time_min").alias("avg_eta_min"),
            )
        )

    @dlt.table(comment="Gold driver daily utilisation")
    def driver_daily_utilisation():
        return (
            dlt.read("silver_trips")
            .groupBy(F.to_date("pickup_time").alias("trip_date"), "driver_id")
            .agg(
                F.count("*").alias("completed_trips"),
                F.sum("duration_min").alias("on_trip_minutes"),
                (F.sum("duration_min") / 480.0).alias("utilisation_rate"),
            )
        )

except ImportError:
    # Local PySpark fallback implementation if DLT package is not present
    logging.warning("DLT module not found. Running in local PySpark Medallion fallback mode.")

    def run_plain_delta_fallback(spark: Any, raw_trips_path: str, silver_output_path: str) -> None:
        df = spark.read.parquet(raw_trips_path)

        # Apply DLT quality rules in standard PySpark filter
        silver_df = (
            df.filter(F.col("fare") > 0)
            .filter(F.col("pickup_zone_id").isNotNull())
            .filter(F.col("pickup_time") >= F.col("request_time"))
            .dropDuplicates(["trip_id"])
        )

        silver_df.write.mode("overwrite").parquet(silver_output_path)
        logging.info("Saved %d valid silver trip records to %s", silver_df.count(), silver_output_path)
