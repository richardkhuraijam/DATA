"""S3 Landing Sync & Glue Crawler Trigger Utility.

Syncs local/HDFS curated Parquet tables to S3:
  s3://<bucket>/curated/<table>/city=<city>/trip_date=<date>/
And triggers AWS Glue Crawler via boto3.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, List

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("s3_sync")


def upload_directory_to_s3(local_dir: Path, bucket: str, s3_prefix: str) -> int:
    region = os.getenv("AWS_REGION", "us-east-1")
    s3_client = boto3.client("s3", region_name=region)
    uploaded_count = 0

    if not local_dir.exists():
        logger.warning("Local directory %s does not exist. Skipping S3 upload.", local_dir)
        return 0

    for file_path in local_dir.rglob("*"):
        if file_path.is_file() and not file_path.name.startswith("."):
            rel_path = file_path.relative_to(local_dir)
            s3_key = f"{s3_prefix.strip('/')}/{rel_path}".replace("\\", "/")
            try:
                s3_client.upload_file(str(file_path), bucket, s3_key)
                uploaded_count += 1
            except Exception as exc:
                logger.error("Failed to upload %s to s3://%s/%s: %s", file_path, bucket, s3_key, exc)

    logger.info("Uploaded %d files to s3://%s/%s", uploaded_count, bucket, s3_prefix)
    return uploaded_count


def trigger_glue_crawler(crawler_name: str = "ridehail_curated_crawler") -> bool:
    region = os.getenv("AWS_REGION", "us-east-1")
    glue = boto3.client("glue", region_name=region)

    try:
        glue.start_crawler(Name=crawler_name)
        logger.info("Triggered AWS Glue crawler '%s' successfully.", crawler_name)
        return True
    except Exception as exc:
        logger.warning("Glue crawler trigger skipped/failed: %s", exc)
        return False


def main() -> None:
    bucket = os.getenv("S3_BUCKET", "")
    if not bucket:
        logger.info("S3_BUCKET environment variable not set. Skipping live S3 sync (AWS Free Tier optional).")
        return

    local_curated = ROOT / "data" / "raw" / "mysql"
    upload_directory_to_s3(local_curated, bucket, "curated")
    trigger_glue_crawler()


if __name__ == "__main__":
    main()
