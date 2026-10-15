"""Latest-weather lookup used inside foreachBatch (replaces the hard-coded rain=0 / temp=20 of the first version)."""
from __future__ import annotations

import logging
import os
from typing import Optional, Protocol

from pyspark.sql import SparkSession, functions as F

logger = logging.getLogger("weather_source")
SCHEMA = "event_id string, event_time string, temperature_c double, rain_mm_h double"


class WeatherProvider(Protocol):
    def latest(self, spark: SparkSession) -> Optional[dict]: ...


def _newest(df) -> Optional[dict]:
    rows = (df.select(F.to_timestamp("event_time").alias("t"), "temperature_c", "rain_mm_h")
              .where(F.col("t").isNotNull()).orderBy(F.desc("t")).limit(1).collect())
    if not rows:
        return None
    r = rows[0]
    return {"rain_mm_h": r["rain_mm_h"], "temp_c": r["temperature_c"], "observed_at": str(r["t"])}


class KafkaWeather:
    def __init__(self, servers: str, topic: str = "weather") -> None:
        self.servers, self.topic = servers, topic

    def latest(self, spark):
        try:
            raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", self.servers).option("subscribe", self.topic)
                   .option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
            return _newest(raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("w")).select("w.*"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("weather read failed: %s", exc); return None


class FileWeather:
    def __init__(self, path: str) -> None:
        self.path = path

    def latest(self, spark):
        try:
            return _newest(spark.read.schema(SCHEMA).json(self.path))
        except Exception as exc:  # noqa: BLE001
            logger.warning("weather read failed: %s", exc); return None


class StaticWeather:
    def __init__(self, rain_mm_h: Optional[float], temp_c: Optional[float]) -> None:
        self._w = {"rain_mm_h": rain_mm_h, "temp_c": temp_c, "observed_at": "static"}

    def latest(self, spark):
        return self._w


def provider_from_env(source: str) -> WeatherProvider:
    if source == "kafka":
        return KafkaWeather(os.getenv("KAFKA_INTERNAL_SERVERS") or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    return FileWeather(os.environ["WEATHER_DIR"])
