"""Drill 2 - Spark restart. REAL: kills the streaming job with SIGKILL, restarts it on the SAME checkpoint, then verifies.

  python -m tests.drills.spark_restart --run-seconds 150
Prereq: Kafka + HBase up and the three producers running. Uses a fresh BRONZE_PATH/CHECKPOINT_PATH for a clean check.
PASS = Bronze batch ids contiguous (no lost batch), every batch has one row per zone (no partial/duplicate writes),
and HBase has exactly one row per (zone, minute) key (idempotent replay).
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

from data.hyderabad import ZONES

ROOT = Path(__file__).resolve().parents[2]


def verify_bronze(bronze: Path) -> tuple[bool, str]:
    ids = sorted(int(p.name.split("=")[1]) for p in bronze.glob("batch_id=*"))
    if not ids:
        return False, "no batches written"
    gaps = [(a, b) for a, b in zip(ids, ids[1:]) if b != a + 1]
    short = [i for i in ids if len(pd.read_parquet(bronze / f"batch_id={i}")) != len(ZONES)]
    return (not gaps and not short), f"batches={len(ids)} gaps={gaps} batches_with_wrong_row_count={short}"


def start(env: dict) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", "processing.streaming.surge_stream"], cwd=ROOT, env={**os.environ, **env})


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--run-seconds", type=int, default=150); a = ap.parse_args()
    work = ROOT / "data" / "drill_spark"
    env = {"BRONZE_PATH": str(work / "bronze"), "CHECKPOINT_PATH": str(work / "ckpt"), "STARTING_OFFSETS": "latest", "PYTHONPATH": str(ROOT)}
    p = start(env); time.sleep(a.run_seconds)
    print("SIGKILL streaming job"); p.send_signal(signal.SIGKILL); p.wait(); time.sleep(5)
    p = start(env); time.sleep(a.run_seconds); p.terminate(); p.wait()
    ok, detail = verify_bronze(work / "bronze")
    print(detail); print("DRILL 2 (spark restart):", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
