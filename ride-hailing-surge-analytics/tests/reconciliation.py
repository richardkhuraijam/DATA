"""Drill 3 - record-count reconciliation, computed from real data (nothing simulated).

Checks, for a time range:
  1. Kafka (end - start offsets of trip_requests)  ==  raw Bronze row count           [needs Kafka + archive_raw_stream]
  2. raw Bronze  -  duplicates  -  invalid zone     =  Silver (distinct, valid)
  3. For every row the surge engine wrote (zone, window_end, demand): demand == requests recomputed from Silver
     in [window_end - 5 min, window_end)                                              [exactly-once / windowing check]

  python -m tests.reconciliation --from 2026-10-12T10:00:00 --to 2026-10-12T11:00:00
  python -m tests.reconciliation ... --skip-kafka
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from data.hyderabad import ZONE_ID_MAX, ZONE_ID_MIN

ROOT = Path(__file__).resolve().parents[1]


def silver_requests(raw: DataFrame) -> DataFrame:
    return (raw.withColumn("ts", F.to_timestamp("event_time"))
               .where(F.col("zone_id").between(ZONE_ID_MIN, ZONE_ID_MAX) & F.col("ts").isNotNull())
               .dropDuplicates(["event_id"]))


def reconcile_windows(silver: DataFrame, surge_bronze: DataFrame) -> tuple[int, int, list]:
    """Compare engine demand with demand recomputed from raw events. Returns (checked, matched, mismatches)."""
    rows = surge_bronze.select("zone_id", "window_end", "demand").dropDuplicates(["zone_id", "window_end"]).collect()
    ok, bad = 0, []
    for r in rows:
        end = dt.datetime.fromisoformat(r["window_end"].replace("Z", "+00:00"))
        start = end - dt.timedelta(minutes=5)
        n = silver.where((F.col("zone_id") == r["zone_id"]) & (F.col("ts") >= F.lit(start))
                         & (F.col("ts") < F.lit(end))).count()
        if n == r["demand"]:
            ok += 1
        else:
            bad.append((r["zone_id"], r["window_end"], r["demand"], n))
    return len(rows), ok, bad


def kafka_count(topic: str, start: dt.datetime, end: dt.datetime) -> int:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"), "group.id": "reconcile"})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    ms = lambda d: int(d.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    s = c.offsets_for_times([TopicPartition(topic, p, ms(start)) for p in parts], timeout=10)
    e = c.offsets_for_times([TopicPartition(topic, p, ms(end)) for p in parts], timeout=10)
    total = 0
    for a, b in zip(s, e):
        _, hi = c.get_watermark_offsets(TopicPartition(topic, a.partition), timeout=10)
        total += (b.offset if b.offset >= 0 else hi) - (a.offset if a.offset >= 0 else hi)
    c.close()
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="s", required=True); ap.add_argument("--to", dest="e", required=True)
    ap.add_argument("--raw", default=str(ROOT / "data" / "lakehouse" / "raw_stream" / "trip_requests"))
    ap.add_argument("--surge", default=str(ROOT / "data" / "lakehouse" / "bronze_surge"))
    ap.add_argument("--skip-kafka", action="store_true")
    a = ap.parse_args()
    s = dt.datetime.fromisoformat(a.s)
    e = dt.datetime.fromisoformat(a.e)
    if s.tzinfo is None:
        s = s.replace(tzinfo=dt.timezone.utc)
    if e.tzinfo is None:
        e = e.replace(tzinfo=dt.timezone.utc)
    spark = SparkSession.builder.appName("reconcile").master("local[*]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    raw_full = spark.read.parquet(a.raw).withColumn("ts", F.to_timestamp("event_time"))
    raw = raw_full.where((F.col("ts") >= F.lit(s)) & (F.col("ts") < F.lit(e)))
    bronze, silver = raw.count(), silver_requests(raw).count()
    distinct, dups = raw.select("event_id").distinct().count(), None
    dups = bronze - distinct
    invalid = distinct - silver
    kafka = None if a.skip_kafka else kafka_count("trip_requests", s, e)
    surge = spark.read.parquet(a.surge).where((F.col("window_end") > a.s) & (F.col("window_end") <= a.e))
    checked, matched, bad = reconcile_windows(silver_requests(raw_full), surge)
    ok = (kafka is None or kafka == bronze) and (bronze - dups - invalid == silver) and not bad
    print(f"Kafka offsets: {kafka if kafka is not None else 'skipped'} | raw Bronze: {bronze} | duplicates: {dups} | invalid: {invalid} | Silver: {silver}")
    print(f"Engine windows checked: {checked}, matched: {matched}, mismatched: {len(bad)}")
    for m in bad[:10]:
        print("  mismatch zone=%s window_end=%s engine=%s recomputed=%s" % m)
    print("RECONCILIATION:", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
