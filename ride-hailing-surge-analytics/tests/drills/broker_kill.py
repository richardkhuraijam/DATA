"""Drill 1 - broker kill. REAL: counts acknowledged produces, Kafka offset deltas and unique consumed event_ids.

  python -m tests.drills.broker_kill --events 6000 --rate 20 --outage-seconds 300
Needs the 3-broker cluster up (`make up-streaming`, `make topics`). Stops container `ridehail-kafka2-1` (override with --container).
PASS = no produce failures AND acknowledged == offset delta == unique consumed ids (zero loss, zero duplicates).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from ingestion.producers.utils import get_bootstrap_servers

TOPIC = "trip_requests"


def end_offsets(consumer: Consumer) -> dict[int, int]:
    parts = consumer.list_topics(TOPIC, timeout=15).topics[TOPIC].partitions
    return {p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=15)[1] for p in parts}


def decide(acked: int, failed: int, delta: int, unique_consumed: int) -> bool:
    return failed == 0 and acked == delta == unique_consumed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=6000); ap.add_argument("--rate", type=float, default=20.0, help="events/second")
    ap.add_argument("--outage-seconds", type=int, default=300); ap.add_argument("--kill-after", type=int, default=10)
    ap.add_argument("--container", default="ridehail-kafka2-1")
    a = ap.parse_args()
    run_id = uuid.uuid4().hex[:8]
    probe = Consumer({"bootstrap.servers": get_bootstrap_servers(), "group.id": f"drill-{run_id}", "auto.offset.reset": "latest"})
    start = end_offsets(probe)
    prod = Producer({"bootstrap.servers": get_bootstrap_servers(), "acks": "all", "enable.idempotence": True})
    state = {"acked": 0, "failed": 0}

    def cb(err, _msg):
        state["failed" if err else "acked"] += 1

    def produce():
        for i in range(a.events):
            prod.produce(TOPIC, key=str(i % 40 + 1), value=json.dumps({"event_id": f"drill-{run_id}-{i}", "event_time": "2026-01-01T00:00:00+00:00", "zone_id": i % 40 + 1}), callback=cb)
            prod.poll(0); time.sleep(1.0 / a.rate)
        prod.flush(120)

    t = threading.Thread(target=produce); t.start()
    time.sleep(a.kill_after)
    print(f"stopping {a.container} for {a.outage_seconds}s"); subprocess.run(["docker", "stop", a.container], check=True)
    time.sleep(a.outage_seconds)
    print(f"starting {a.container}"); subprocess.run(["docker", "start", a.container], check=True)
    t.join(); time.sleep(15)
    end = end_offsets(probe)
    delta = sum(end[p] - start.get(p, 0) for p in end)
    ids = set()
    c = Consumer({"bootstrap.servers": get_bootstrap_servers(), "group.id": f"drill-read-{run_id}", "enable.auto.commit": False})
    c.assign([TopicPartition(TOPIC, p, start.get(p, 0)) for p in end]); idle = 0
    while idle < 5:
        m = c.poll(2.0)
        if m is None:
            idle += 1; continue
        if m.error() and m.error().code() != KafkaError._PARTITION_EOF:
            continue
        if not m.error():
            ev = json.loads(m.value())
            if str(ev.get("event_id", "")).startswith(f"drill-{run_id}"):
                ids.add(ev["event_id"])
                idle = 0
                if state["acked"] > 0 and len(ids) == state["acked"]:
                    break
            else:
                idle += 1
    ok = decide(state["acked"], state["failed"], delta, len(ids))
    print(f"produced_acked={state['acked']} failed={state['failed']} offset_delta={delta} unique_consumed={len(ids)}")
    print("DRILL 1 (broker kill):", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
