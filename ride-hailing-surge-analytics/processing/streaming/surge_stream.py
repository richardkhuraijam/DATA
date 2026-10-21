"""Surge engine - PySpark Structured Streaming.

Kafka (trip_requests + driver_gps_pings) -> one unified event stream -> 10-min watermark + dropDuplicates(event_id)
-> demand/supply per zone on a 5-minute window sliding every 1 minute -> foreachBatch every 60 s
(weather join, surge rules, HBase + Kafka + Bronze).

What was wrong in the first version, and what is done now:
  * countDistinct is illegal on streams            -> approx_count_distinct (HLL++, effectively exact at these sizes)
  * stream-stream FULL OUTER join of 2 aggregations -> ONE aggregation over a union of both streams (conditional aggs)
  * weather hard-coded (rain 0, 20 C)               -> latest Open-Meteo reading joined in foreachBatch
  * smoothing never applied                         -> previous-minute multiplier read from HBase
  * Scala-2.13 connector jars on a 2.12 PySpark      -> spark-sql-kafka-0-10_2.12:3.5.3
  * silent fallbacks in sinks                       -> failures surface and Spark retries the batch

Run (host, Kafka at localhost):  python -m processing.streaming.surge_stream
Local test without Kafka:        SURGE_SOURCE=files REQUESTS_DIR=.. PINGS_DIR=.. WEATHER_DIR=.. SURGE_STORE=memory
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from processing.streaming.sinks import SinkConfig, make_batch_writer
from processing.streaming.weather_source import provider_from_env
from storage.hbase_schema import get_store

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("surge_stream")

WATERMARK = "10 minutes"
REQ_SCHEMA = T.StructType([T.StructField("event_id", T.StringType()), T.StructField("event_time", T.StringType()),
                           T.StructField("zone_id", T.IntegerType())])
PING_SCHEMA = T.StructType([T.StructField("event_id", T.StringType()), T.StructField("event_time", T.StringType()),
                            T.StructField("driver_id", T.StringType()), T.StructField("zone_id", T.IntegerType()),
                            T.StructField("status", T.StringType())])


def spark_session(source: str, bronze_format: str) -> SparkSession:
    b = (SparkSession.builder.appName("SurgeEngine").master(os.getenv("SPARK_MASTER", "local[*]"))
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g")))
    pk = []
    if source == "kafka":
        pk.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    if bronze_format == "delta":
        pk.append("io.delta:delta-spark_2.12:3.2.0")
        b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if pk:
        b = b.config("spark.jars.packages", os.getenv("SPARK_PACKAGES", ",".join(pk)))
    return b.getOrCreate()


def _kafka(spark: SparkSession, servers: str, topic: str, schema: T.StructType) -> DataFrame:
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", servers).option("subscribe", topic)
           .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest")).option("failOnDataLoss", "false").load())
    return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d")).select("d.*")


def read_sources(spark: SparkSession, source: str) -> tuple[DataFrame, DataFrame]:
    if source == "kafka":
        servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")
        return _kafka(spark, servers, "trip_requests", REQ_SCHEMA), _kafka(spark, servers, "driver_gps_pings", PING_SCHEMA)
    return (spark.readStream.schema(REQ_SCHEMA).json(os.environ["REQUESTS_DIR"]),
            spark.readStream.schema(PING_SCHEMA).json(os.environ["PINGS_DIR"]))


def unified_events(requests: DataFrame, pings: DataFrame) -> DataFrame:
    req = (requests.select("event_id", F.to_timestamp("event_time").alias("event_time"), "zone_id",
                           F.lit("request").alias("kind"), F.lit(None).cast("string").alias("driver_id"),
                           F.lit(None).cast("string").alias("status")))
    png = pings.select("event_id", F.to_timestamp("event_time").alias("event_time"), "zone_id",
                       F.lit("ping").alias("kind"), "driver_id", F.upper("status").alias("status"))
    return req.unionByName(png).where(F.col("zone_id").isNotNull() & F.col("event_time").isNotNull())


def windowed_aggregate(events: DataFrame) -> DataFrame:
    ev = events.withWatermark("event_time", WATERMARK).dropDuplicates(["event_id", "event_time"])
    ping = F.col("kind") == "ping"
    return ev.groupBy(F.window("event_time", "5 minutes", "1 minute").alias("window"), "zone_id").agg(
        F.count(F.when(F.col("kind") == "request", 1)).alias("demand"),
        F.approx_count_distinct(F.when(ping & (F.col("status") == "AVAILABLE"), F.col("driver_id")), 0.01).alias("drivers_available"),
        F.approx_count_distinct(F.when(ping & (F.col("status") == "ON_TRIP"), F.col("driver_id")), 0.01).alias("drivers_on_trip"))


def build_query(spark: SparkSession, source: str, cfg: SinkConfig, once: bool = False, checkpoint: Optional[str] = None) -> Any:
    requests, pings = read_sources(spark, source)
    agg = windowed_aggregate(unified_events(requests, pings))
    w = (agg.writeStream.outputMode("update").foreachBatch(make_batch_writer(cfg))
         .option("checkpointLocation", checkpoint or os.getenv("CHECKPOINT_PATH", str(ROOT / "data" / "checkpoints" / "surge"))))
    if once:
        return w.trigger(availableNow=True).start()
    return w.trigger(processingTime=f"{os.getenv('TRIGGER_SECONDS', '60')} seconds").start()


def main() -> None:
    source = os.getenv("SURGE_SOURCE", "kafka")
    bronze_format = os.getenv("BRONZE_FORMAT", "parquet")
    spark = spark_session(source, bronze_format)
    spark.sparkContext.setLogLevel("WARN")
    cfg = SinkConfig(store_factory=get_store, weather=provider_from_env(source),
                     bronze_path=os.getenv("BRONZE_PATH", str(ROOT / "data" / "lakehouse" / "bronze_surge")),
                     bronze_format=bronze_format,
                     kafka_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS") if (source == "kafka" and os.getenv("SINK_KAFKA", "1") == "1") else None)
    q = build_query(spark, source, cfg)
    logger.info("surge engine running (source=%s, store=%s, bronze=%s)", source, os.getenv("SURGE_STORE", "hbase"), bronze_format)
    try:
        q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("stopped")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
