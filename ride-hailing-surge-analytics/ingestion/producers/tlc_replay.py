"""TLC Trip Request Replay Producer.

Reads historical TLC HVFHV Parquet files and replays them as live trip_requests events
re-stamped to the current time, with configurable replay speed factor (1x to 60x).
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, Generator, Optional
import uuid

import pandas as pd
from dotenv import load_dotenv

from ingestion.producers.utils import create_producer, delivery_report

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("tlc_replay")


def format_trip_request_event(
    row: Dict[str, Any], current_time: Optional[datetime] = None
) -> Dict[str, Any]:
    now = current_time or datetime.now(timezone.utc)
    pu_zone = int(row.get("PULocationID") or row.get("pickup_zone_id") or 1)
    do_zone = int(row.get("DOLocationID") or row.get("dropoff_zone_id") or 1)

    event_id = str(uuid.uuid4())
    trip_id = str(row.get("hvfhs_license_num", "TLC") + "_" + str(uuid.uuid4())[:8])

    return {
        "event_id": event_id,
        "event_time": now.isoformat(),
        "trip_id": trip_id,
        "zone_id": pu_zone,
        "dropoff_zone_id": do_zone,
        "distance_miles": float(row.get("trip_miles") or 1.0),
        "fare": float(row.get("base_passenger_fare") or 10.0),
        "source": "tlc_replay",
    }


def stream_tlc_parquet(
    parquet_dir: Path, speed_factor: float = 10.0, max_events: Optional[int] = None
) -> Generator[Dict[str, Any], None, None]:
    files = list(parquet_dir.glob("*.parquet"))
    if not files:
        logger.warning("No TLC parquet files found in %s", parquet_dir)
        return

    logger.info("Found %d TLC parquet files. Starting replay at speed %.1fx...", len(files), speed_factor)
    events_count = 0

    for file_path in files:
        df = pd.read_parquet(file_path, columns=["PULocationID", "DOLocationID", "trip_miles", "base_passenger_fare"])
        for _, row in df.iterrows():
            event = format_trip_request_event(row.to_dict())
            yield event
            events_count += 1
            if max_events and events_count >= max_events:
                return
            if speed_factor > 0:
                time.sleep(1.0 / (100.0 * speed_factor))


def main() -> None:
    tlc_dir = ROOT / "data" / "raw" / "tlc"
    speed = float(os.getenv("REPLAY_SPEED_FACTOR", "10.0"))
    topic = "trip_requests"

    producer = create_producer()
    logger.info("Publishing trip requests to Kafka topic '%s'...", topic)

    published = 0
    try:
        for event in stream_tlc_parquet(tlc_dir, speed_factor=speed, max_events=1000):
            key = str(event["zone_id"])
            producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            published += 1
            if published % 100 == 0:
                logger.info("Published %d trip requests...", published)
    except KeyboardInterrupt:
        logger.info("TLC replay interrupted by user.")
    finally:
        producer.flush(timeout=5)
        logger.info("TLC replay completed. Total published: %d", published)


if __name__ == "__main__":
    main()
