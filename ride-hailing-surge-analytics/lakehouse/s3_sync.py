"""Upload the curated warehouse to S3 (partitioned Snappy Parquet) and a raw-CSV baseline for the Athena benchmark,
then optionally start the Glue crawler.

  s3://<bucket>/curated/fact_trips/city=Hyderabad/trip_date=YYYY-MM-DD/*.parquet   (+ dims)
  s3://<bucket>/raw_csv/fact_trips_csv/fact_trips.csv                               (single unpartitioned CSV)
Point the Glue crawler at BOTH prefixes. Credentials come from the standard AWS chain (aws configure / env vars).
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
logger = logging.getLogger("s3_sync")


def upload_dir(local: Path, bucket: str, prefix: str) -> int:
    s3, n = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-south-1")), 0
    for f in sorted(local.rglob("*")):
        if f.is_file() and not f.name.startswith((".", "_")) and not f.name.endswith(".crc"):
            s3.upload_file(str(f), bucket, f"{prefix.strip('/')}/{f.relative_to(local).as_posix()}"); n += 1
    logger.info("uploaded %d files to s3://%s/%s", n, bucket, prefix)
    return n


def write_csv_baseline(warehouse: Path, tmp: Path) -> Path:
    df = pd.read_parquet(warehouse / "fact_trips")
    df["trip_date"] = df["trip_date"].astype(str)
    tmp.mkdir(parents=True, exist_ok=True)
    df.drop(columns=["city"], errors="ignore").to_csv(tmp / "fact_trips.csv", index=False)
    return tmp


def start_crawler(name: str | None = None) -> None:
    boto3.client("glue", region_name=os.getenv("AWS_REGION", "ap-south-1")).start_crawler(Name=name or os.getenv("GLUE_CRAWLER", "ridehail_curated_crawler"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(); ap.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse")); ap.add_argument("--crawler", action="store_true")
    a = ap.parse_args()
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise SystemExit("S3_BUCKET is not set (.env). Nothing uploaded.")
    wh = Path(a.warehouse)
    for t in ("fact_trips", "dim_zone", "dim_driver", "dim_time"):
        upload_dir(wh / t, bucket, f"curated/{t}")
    upload_dir(write_csv_baseline(wh, ROOT / "data" / "_csv_baseline"), bucket, "raw_csv/fact_trips_csv")
    if a.crawler:
        start_crawler()


if __name__ == "__main__":
    main()
