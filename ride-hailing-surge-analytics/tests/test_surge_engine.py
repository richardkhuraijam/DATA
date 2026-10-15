"""End-to-end test of the REAL Structured Streaming query on a local file source (no Kafka needed):
windowing, dedup, distinct supply, weather join, surge rules, smoothing and the sinks."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession

from processing.streaming import surge_stream
from processing.streaming.sinks import SinkConfig, build_output_rows
from processing.streaming.weather_source import StaticWeather
from storage.hbase_schema import MemoryStore

T0 = datetime(2026, 10, 12, 13, 0, 0, tzinfo=timezone.utc)      # 18:30 IST


def iso(offset_s: float) -> str:
    return (T0 + timedelta(seconds=offset_s)).isoformat()


def write_jsonl(path, rows) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows))


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("t").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def run_engine(spark, tmp_path, requests, pings, weather, store):
    (tmp_path / "req").mkdir(); (tmp_path / "png").mkdir()
    write_jsonl(tmp_path / "req" / "a.json", requests); write_jsonl(tmp_path / "png" / "a.json", pings)
    cfg = SinkConfig(store_factory=lambda: store, weather=weather, bronze_path=str(tmp_path / "bronze"), bronze_format="parquet")
    import os
    os.environ.update(REQUESTS_DIR=str(tmp_path / "req"), PINGS_DIR=str(tmp_path / "png"))
    q = surge_stream.build_query(spark, "files", cfg, once=True, checkpoint=str(tmp_path / "ckpt"))
    q.awaitTermination(180)
    assert q.exception() is None, q.exception()


def test_engine_end_to_end(spark, tmp_path):
    store = MemoryStore()
    reqs, n = [], 0
    def req(zone, off):
        nonlocal n; n += 1
        return {"event_id": f"r{n}", "event_time": iso(off), "zone_id": zone}
    # zone 1 (Hitech City): 30 requests, only 2 distinct AVAILABLE drivers (+3 on trip) -> ratio 15 -> 2.5x (+0.2 rain)
    reqs += [req(1, -290 + i * 7) for i in range(30)]
    # zone 2: 10 requests (+5 DUPLICATES of the same event ids), 10 available drivers -> ratio 1.0 -> 1.2x (+0.2)
    zone2 = [req(2, -280 + i * 20) for i in range(10)]
    reqs += zone2 + zone2[:5]
    pings = []
    def ping(driver, zone, status, off, i=[0]):
        i[0] += 1
        return {"event_id": f"p{i[0]}", "event_time": iso(off), "driver_id": driver, "zone_id": zone, "status": status}
    for off in (-250, -200, -150, -100, -70):                      # same 2 drivers ping repeatedly: must count once
        pings += [ping("D1", 1, "AVAILABLE", off), ping("D2", 1, "AVAILABLE", off)]
    pings += [ping(f"T{k}", 1, "ON_TRIP", -120) for k in range(3)]
    pings += [ping(f"Z{k}", 2, "AVAILABLE", -150) for k in range(10)]
    pings += [ping(f"G{k}", 3, "AVAILABLE", -150) for k in range(5)]  # zone 3: supply but no demand
    pings += [ping("S1", 40, "AVAILABLE", -20)]                       # sentinel: newest event defines "now"
    run_engine(spark, tmp_path, reqs, pings, StaticWeather(5.0, 30.0), store)

    cur = {int(k[:3]): v for k, v in store.rows.items()}
    assert len(cur) == 40, "every zone gets a row"
    z1, z2, z3 = cur[1], cur[2], cur[3]
    assert (z1["demand"], z1["supply"], z1["on_trip"]) == ("30", "2", "3")
    assert float(z1["ratio"]) == 15.0 and float(z1["multiplier"]) == 2.7          # 2.5x base + 0.2x rain
    assert (z2["demand"], z2["supply"]) == ("10", "10"), "dedup + distinct drivers"
    assert float(z2["multiplier"]) == 1.4                                            # 1.2x + 0.2x rain
    assert z3["demand"] == "0" and z3["supply"] == "5" and float(z3["multiplier"]) == 1.2   # 1.0x + 0.2x rain
    assert cur[20]["demand"] == "0" and cur[20]["supply"] == "0"                    # silent zone is written as no-surge
    assert z1["rain_mm"] == "5.0" and z1["temp_c"] == "30.0"
    assert all(k.endswith("202610121259") for k in store.rows if k.startswith("001#")), store.rows.keys()
    import pandas as pd
    assert len(pd.read_parquet(tmp_path / "bronze")) == 40                           # Bronze written


def test_smoothing_across_minutes_and_missing_weather():
    store = MemoryStore()
    store.put_surge(1, "2026-10-12T12:58:00Z", 1.0, 5, 5, 1.0)
    cur = [{"zone_id": 1, "window_end": "2026-10-12T12:59:00Z", "demand": 100, "drivers_available": 1, "drivers_on_trip": 0}]
    rows = build_output_rows(cur, "2026-10-12T12:59:00Z", None, store, batch_id=7)
    z1 = next(r for r in rows if r["zone_id"] == 1)
    assert z1["surge_multiplier"] == 1.3 and z1["rain_mm_h"] is None             # +0.3 cap per minute, no weather adj
    assert z1["batch_id"] == 7


def test_reconciliation_matches_engine_output(spark, tmp_path):
    """The engine's window demand must equal demand recomputed from the raw events (exactly-once / windowing check)."""
    from tests.reconciliation import reconcile_windows, silver_requests
    store = MemoryStore()
    reqs = [{"event_id": f"x{i}", "event_time": iso(-280 + i * 6), "zone_id": 1 + i % 3} for i in range(40)]
    reqs += reqs[:6]                                                                      # duplicates
    pings = [{"event_id": "pp", "event_time": iso(-20), "driver_id": "S", "zone_id": 40, "status": "AVAILABLE"}]
    run_engine(spark, tmp_path, reqs, pings, StaticWeather(0.0, 28.0), store)
    raw = spark.read.schema("event_id string, event_time string, zone_id int").json(str(tmp_path / "req")).withColumn("ts", __import__("pyspark.sql.functions", fromlist=["x"]).to_timestamp("event_time"))
    surge = spark.read.parquet(str(tmp_path / "bronze"))
    checked, matched, bad = reconcile_windows(silver_requests(raw), surge)
    assert checked == 40 and not bad and matched == 40, bad
