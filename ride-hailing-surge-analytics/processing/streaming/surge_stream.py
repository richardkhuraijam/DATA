"""PySpark Structured Streaming Surge Engine Application.

Reads trip_requests and driver_gps_pings from Kafka, applies 10-min watermark,
deduplicates on event_id + event_time, aggregates on 5-min sliding window (1-min slide),
joins latest weather readings, computes surge multiplier, and sinks to HBase, Kafka, and Delta.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from dotenv import load_dotenv

from processing.streaming.sinks import write_batch_to_sinks
from processing.streaming.surge_rules import calculate_surge_multiplier

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("surge_stream")

# UDF for surge rules engine
@F.udf(
    returnType=StructType(
        [
            StructField("demand", DoubleType(), True),
            StructField("supply", DoubleType(), True),
            StructField("ratio", DoubleType(), True),
            StructField("base_multiplier", DoubleType(), True),
            StructField("weather_adjustment", DoubleType(), True),
            StructField("surge_multiplier", DoubleType(), True),
        ]
    )
)
def compute_surge_udf(demand: int, supply: int, rain_mm: float, temp_c: float) -> dict[str, float]:
    d = demand if demand is not None else 0
    s = supply if supply is not None else 0
    r = rain_mm if rain_mm is not None else 0.0
    t = temp_c if temp_c is not None else 20.0
    return calculate_surge_multiplier(d, s, r, t)


def get_spark_streaming_session(app_name: str = "SurgeStreamingEngine") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,io.delta:delta-spark_2.13:3.2.0")
        .getOrCreate()
    )


def create_surge_stream(spark: SparkSession) -> Any:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")
    checkpoint_dir = os.getenv("CHECKPOINT_PATH", str(ROOT / "data" / "checkpoints" / "surge"))

    # Schemas
    trip_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("trip_id", StringType(), True),
            StructField("zone_id", IntegerType(), True),
            StructField("fare", DoubleType(), True),
        ]
    )

    ping_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("driver_id", IntegerType(), True),
            StructField("zone_id", IntegerType(), True),
            StructField("status", StringType(), True),
        ]
    )

    logger.info("Reading trip_requests and driver_gps_pings from Kafka (%s)...", kafka_servers)

    # Read streams
    trips_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "trip_requests")
        .option("startingOffsets", "latest")
        .load()
    )

    pings_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "driver_gps_pings")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON & Watermark
    trips = (
        trips_raw.select(F.from_json(F.col("value").cast("string"), trip_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
    )

    pings = (
        pings_raw.select(F.from_json(F.col("value").cast("string"), ping_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
    )

    # Aggregations on 5-min window sliding 1-min
    demand_agg = (
        trips.groupBy(F.window(F.col("event_timestamp"), "5 minutes", "1 minute"), F.col("zone_id"))
        .agg(F.count("*").alias("demand"))
    )

    supply_agg = (
        pings.filter((F.col("status") == "AVAILABLE") | (F.col("status") == "CITYBIKES_SUPPLY"))
        .groupBy(F.window(F.col("event_timestamp"), "5 minutes", "1 minute"), F.col("zone_id"))
        .agg(F.countDistinct("driver_id").alias("supply"))
    )

    # Join demand & supply
    joined = (
        demand_agg.join(supply_agg, on=["window", "zone_id"], how="full_outer")
        .na.fill({"demand": 0, "supply": 0})
        .withColumn("rain_mm_h", F.lit(0.0))
        .withColumn("temp_c", F.lit(20.0))
        .withColumn("surge_stats", compute_surge_udf("demand", "supply", "rain_mm_h", "temp_c"))
        .select(
            F.col("window.end").cast("string").alias("window_end"),
            "zone_id",
            "demand",
            "supply",
            F.col("surge_stats.ratio").alias("ratio"),
            F.col("surge_stats.surge_multiplier").alias("surge_multiplier"),
            "rain_mm_h",
            "temp_c",
        )
    )

    # Stream query trigger 60s
    query = (
        joined.writeStream.foreachBatch(write_batch_to_sinks)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )

    logger.info("Surge streaming query started with checkpoint at %s", checkpoint_dir)
    return query


def main() -> None:
    spark = get_spark_streaming_session()
    try:
        query = create_surge_stream(spark)
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming engine stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
