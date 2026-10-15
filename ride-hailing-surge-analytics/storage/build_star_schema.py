"""Build the Hive star schema (dim_driver, dim_zone, dim_time, fact_trips) from the cleaned MySQL extracts.

  python -m storage.build_star_schema                       # writes Parquet under data/warehouse
  python -m storage.build_star_schema --register-hive       # also creates/repairs the Hive external tables
Reads data/raw/mysql/{trips,drivers,city_zones} (from Sqoop or ingestion.sqoop.spark_jdbc_import).
Driver names are hashed (SHA-256) in the warehouse - no raw personal identifiers are propagated.
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from data.hyderabad import CITY

ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger("star_schema")
PEAK_HOURS = [8, 9, 10, 17, 18, 19, 20]


def build(spark: SparkSession, raw: str, warehouse: str, city: str = CITY) -> dict[str, int]:
    trips = spark.read.parquet(f"{raw}/trips")
    valid = (trips.where(F.col("pickup_zone_id").isNotNull() & F.col("dropoff_zone_id").isNotNull())
                  .where(F.col("fare").isNull() | (F.col("fare") >= 0))
                  .where(F.col("pickup_time").isNull() | F.col("dropoff_time").isNull() | (F.col("dropoff_time") >= F.col("pickup_time"))))
    drivers = spark.read.parquet(f"{raw}/drivers")
    dim_driver = (drivers.withColumn("driver_key", F.row_number().over(Window.orderBy("driver_id")))
                  .select("driver_key", "driver_id", F.sha2("name", 256).alias("name_hash"), "vehicle_type", "joined_date",
                          "home_city", F.col("rating").cast("double").alias("rating")))
    zones = spark.read.parquet(f"{raw}/city_zones")
    dim_zone = zones.select(F.col("zone_id").alias("zone_key"), "zone_id", "zone_name", F.col("borough").alias("zone_type"),
                            F.lit(city).alias("city"), "centroid_lat", "centroid_lon")
    mins = valid.select(F.date_trunc("minute", "request_time").alias("full_ts")).distinct()
    dim_time = mins.select(F.date_format("full_ts", "yyyyMMddHHmm").cast("bigint").alias("time_key"), "full_ts",
                           F.to_date("full_ts").alias("date"), F.hour("full_ts").alias("hour"), F.minute("full_ts").alias("minute"),
                           F.dayofweek("full_ts").alias("day_of_week"), F.dayofweek("full_ts").isin(1, 7).alias("is_weekend"),
                           F.lit(False).alias("is_holiday"), F.hour("full_ts").isin(PEAK_HOURS).alias("peak_flag"))
    fact = (valid.join(dim_driver.select("driver_id", "driver_key"), "driver_id", "left")
            .select("trip_id", "driver_key", "rider_id", F.col("pickup_zone_id").alias("pickup_zone_key"),
                    F.col("dropoff_zone_id").alias("dropoff_zone_key"),
                    F.date_format(F.date_trunc("minute", "request_time"), "yyyyMMddHHmm").cast("bigint").alias("time_key"),
                    F.col("status").alias("trip_status"), "cancel_reason", "distance_km",
                    ((F.unix_timestamp("dropoff_time") - F.unix_timestamp("pickup_time")) / 60).alias("duration_min"),
                    F.col("fare").cast("double").alias("fare_inr"),
                    ((F.unix_timestamp("pickup_time") - F.unix_timestamp("request_time")) / 60).alias("wait_time_min"),
                    F.lit(city).alias("city"), F.to_date("request_time").alias("trip_date")))
    for name, df in (("dim_driver", dim_driver), ("dim_zone", dim_zone), ("dim_time", dim_time)):
        df.write.mode("overwrite").parquet(f"{warehouse}/{name}")
    fact.write.mode("overwrite").partitionBy("city", "trip_date").parquet(f"{warehouse}/fact_trips")
    counts = {n: spark.read.parquet(f"{warehouse}/{n}").count() for n in ("dim_driver", "dim_zone", "dim_time", "fact_trips")}
    logger.info("star schema built: %s", counts)
    return counts


def register_hive(spark: SparkSession, warehouse: str) -> None:
    ddl = (Path(__file__).with_name("hive_ddl.sql")).read_text().replace("${hivevar:warehouse}", warehouse)
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        spark.sql(stmt)
    spark.sql("MSCK REPAIR TABLE ridehail_dw.fact_trips")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=os.getenv("RAW_LANDING_BASE", str(ROOT / "data" / "raw" / "mysql")))
    ap.add_argument("--warehouse", default=os.getenv("WAREHOUSE_DIR", str(ROOT / "data" / "warehouse")))
    ap.add_argument("--register-hive", action="store_true")
    a = ap.parse_args()
    b = SparkSession.builder.appName("star_schema").master("local[*]").config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    if a.register_hive:
        b = b.enableHiveSupport()
    spark = b.getOrCreate()
    try:
        print(build(spark, a.raw, a.warehouse))
        if a.register_hive:
            register_hive(spark, a.warehouse); print("Hive tables registered in ridehail_dw")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
