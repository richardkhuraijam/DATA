"""CityBikes API Live Supply Proxy Producer.

Polls the Citi Bike NYC network (citibike-nyc) via API every 60s.
Maps bike station lat/lon to TLC taxi zone_id and publishes supply proxy events to Kafka.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import uuid

from dotenv import load_dotenv
import requests

from data.zones import NYCZones
from ingestion.producers.utils import create_producer, delivery_report

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("citybikes")

CITYBIKES_URL = "https://api.citybik.es/v2/networks/citi-bike-nyc"


def fetch_citybikes_stations() -> List[Dict[str, Any]]:
    try:
        resp = requests.get(CITYBIKES_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("network", {}).get("stations", [])
    except Exception as exc:
        logger.warning("Error fetching CityBikes API: %s", exc)
        return []


def format_citybikes_event(
    station: Dict[str, Any], zones_helper: NYCZones
) -> Optional[Dict[str, Any]]:
    lat = station.get("latitude")
    lon = station.get("longitude")
    if lat is None or lon is None:
        return None

    zone_id = zones_helper.lat_lon_to_zone_id(lat, lon)
    if zone_id is None:
        return None

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "station_id": str(station.get("id")),
        "free_bikes": int(station.get("free_bikes") or 0),
        "empty_slots": int(station.get("empty_slots") or 0),
        "lat": lat,
        "lon": lon,
        "zone_id": zone_id,
        "status": "CITYBIKES_SUPPLY",
        "source": "citybikes",
    }


def main() -> None:
    topic = "driver_gps_pings"
    producer = create_producer()
    zones_helper = NYCZones()

    logger.info("Starting CityBikes supply proxy producer polling every 60s...")

    try:
        while True:
            stations = fetch_citybikes_stations()
            published = 0
            for station in stations:
                event = format_citybikes_event(station, zones_helper)
                if event is None:
                    continue

                key = str(event["zone_id"])
                producer.produce(
                    topic=topic,
                    key=key,
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                published += 1

            producer.flush(timeout=2)
            logger.info("Published %d CityBikes supply events across NYC zones.", published)
            time.sleep(60.0)
    except KeyboardInterrupt:
        logger.info("CityBikes producer stopped by user.")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    main()
