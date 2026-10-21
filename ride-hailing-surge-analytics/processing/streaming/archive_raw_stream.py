"""Land the raw Kafka topics as Parquet (immutable raw Bronze) for replay, reconciliation and utilisation analytics.
Writes data/lakehouse/raw_stream/<topic>/ with Kafka partition + offset so counts can be reconciled exactly."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = {"trip_requests": "event_id string, event_time string, trip_id string, zone_id int, dropoff_zone_id int, distance_km double, fare double, source string",
           "driver_gps_pings": "event_id string, event_time string, driver_id string, zone_id int, lat double, lon double, status string, source string",
           "weather": "event_id string, event_time string, city string, temperature_c double, rain_mm_h double, precipitation_mm_h double, weather_code int, source string"}


def main() -> None:
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")
    base = os.getenv("RAW_STREAM_PATH", str(ROOT / "data" / "lakehouse" / "raw_stream"))
    spark = (SparkSession.builder.appName("archive_raw").master("local[*]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3").getOrCreate())
    for topic, schema in SCHEMAS.items():
        df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", servers).option("subscribe", topic)
              .option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
              .select(F.from_json(F.col("value").cast("string"), schema).alias("e"), F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset")).select("e.*", "kafka_partition", "kafka_offset"))
        (df.writeStream.format("parquet").option("path", f"{base}/{topic}").option("checkpointLocation", f"{base}/_ckpt/{topic}")
           .trigger(processingTime="30 seconds").start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
