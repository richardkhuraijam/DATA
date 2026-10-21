"""foreachBatch sink: pick each zone's current window, join latest weather, apply surge rules (with smoothing),
then write to HBase, Kafka `surge_updates` (optional) and the Bronze layer (Delta or per-batch Parquet).

Exactly-once on restart:
  * HBase: row key zone_id#minute is deterministic -> a replayed batch overwrites the same cells.
  * Delta: txnAppId + txnVersion=batch_id -> a replayed batch is skipped.  Parquet mode: batch_id=<id> folder is overwritten.
  * Smoothing reads the PREVIOUS minute's row, so a replayed batch recomputes identical values.
Nothing is swallowed: if a sink fails the batch fails and Spark retries it from the checkpoint.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, Window, functions as F, types as T

from data.hyderabad import ZONES
from processing.streaming.surge_rules import calculate_surge_multiplier
from processing.streaming.weather_source import WeatherProvider

logger = logging.getLogger("surge_sinks")
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

OUT_SCHEMA = T.StructType([
    T.StructField("zone_id", T.IntegerType()), T.StructField("window_end", T.StringType()),
    T.StructField("demand", T.IntegerType()), T.StructField("supply", T.IntegerType()), T.StructField("on_trip", T.IntegerType()),
    T.StructField("ratio", T.DoubleType()), T.StructField("surge_multiplier", T.DoubleType()),
    T.StructField("rain_mm_h", T.DoubleType()), T.StructField("temp_c", T.DoubleType()),
    T.StructField("processed_at", T.StringType()), T.StructField("batch_id", T.LongType())])


@dataclass
class SinkConfig:
    store_factory: Callable[[], Any]
    weather: WeatherProvider
    bronze_path: Optional[str] = None
    bronze_format: str = "parquet"          # delta | parquet
    kafka_servers: Optional[str] = None     # set to publish surge_updates


def select_current_windows(batch_df: DataFrame) -> tuple[list[dict], Optional[str]]:
    """One row per zone: among windows touched by this batch that end at/after the newest minute boundary R,
    take the earliest end. That window holds ~4-5 minutes of history and is complete up to now.
    (The old code took the LATEST end, which only contains the newest minute -> demand biased low.)"""
    d = batch_df.select(F.col("window.start").alias("ws"), F.col("window.end").alias("we"), "zone_id", "demand",
                        "drivers_available", "drivers_on_trip")
    r = d.agg(F.max("ws").alias("r")).first()["r"]
    if r is None:
        return [], None
    w = Window.partitionBy("zone_id").orderBy("we")
    cur = (d.where(F.col("we") >= F.lit(r)).withColumn("rn", F.row_number().over(w)).where("rn = 1")
             .select("zone_id", F.date_format("we", TS_FMT).alias("window_end"), "demand", "drivers_available", "drivers_on_trip"))
    ref_end = d.select(F.date_format(F.lit(r) + F.expr("INTERVAL 1 MINUTE"), TS_FMT)).first()[0]
    return [x.asDict() for x in cur.collect()], ref_end


def build_output_rows(current: list[dict], ref_end: str, weather: Optional[dict], store: Any, batch_id: int) -> list[dict]:
    """Pure Python (unit-testable): weather join + surge rules + smoothing for every known zone."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rain = weather["rain_mm_h"] if weather else None
    temp = weather["temp_c"] if weather else None
    seen = {c["zone_id"]: c for c in current}
    out = []
    for z in ZONES:
        c = seen.get(z.zone_id)
        demand = int(c["demand"] or 0) if c else 0
        supply = int(c["drivers_available"] or 0) if c else 0
        on_trip = int(c["drivers_on_trip"] or 0) if c else 0
        wend = c["window_end"] if c else ref_end
        prev = store.previous_multiplier(z.zone_id, wend)
        res = calculate_surge_multiplier(demand, supply, rain, temp, prev)
        out.append({"zone_id": z.zone_id, "window_end": wend, "demand": demand, "supply": supply, "on_trip": on_trip,
                    "ratio": res["ratio"], "surge_multiplier": res["surge_multiplier"], "rain_mm_h": rain, "temp_c": temp,
                    "processed_at": now, "batch_id": int(batch_id)})
    return out


def make_batch_writer(cfg: SinkConfig) -> Callable[[DataFrame, int], None]:
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("batch %d empty", batch_id); return
        spark = batch_df.sparkSession
        current, ref_end = select_current_windows(batch_df)
        if ref_end is None:
            return
        weather = cfg.weather.latest(spark)
        if weather is None:
            logger.warning("batch %d: no weather available - no weather adjustment applied", batch_id)
        store = cfg.store_factory()
        try:
            rows = build_output_rows(current, ref_end, weather, store, batch_id)
            for r in rows:                                              # 1) HBase (idempotent by key)
                store.put_surge(r["zone_id"], r["window_end"], r["surge_multiplier"], r["demand"], r["supply"], r["ratio"],
                                r["rain_mm_h"], r["temp_c"], r["on_trip"])
        finally:
            store.close()
        out_df = spark.createDataFrame([tuple(r[f.name] for f in OUT_SCHEMA.fields) for r in rows], OUT_SCHEMA)
        if cfg.kafka_servers:                                           # 2) Kafka surge_updates
            (out_df.select(F.col("zone_id").cast("string").alias("key"), F.to_json(F.struct("*")).alias("value"))
                   .write.format("kafka").option("kafka.bootstrap.servers", cfg.kafka_servers).option("topic", "surge_updates").save())
        if cfg.bronze_path:                                             # 3) Bronze
            if cfg.bronze_format == "delta":
                (out_df.write.format("delta").mode("append").option("txnAppId", "surge_bronze")
                       .option("txnVersion", int(batch_id)).save(cfg.bronze_path))
            else:
                out_df.drop("batch_id").write.mode("overwrite").parquet(f"{cfg.bronze_path}/batch_id={int(batch_id)}")
        hot = max(rows, key=lambda r: r["surge_multiplier"])
        logger.info("batch %d: %d zones, top surge %.2fx in zone %d (demand %d / supply %d)", batch_id, len(rows),
                    hot["surge_multiplier"], hot["zone_id"], hot["demand"], hot["supply"])
    return write_batch
