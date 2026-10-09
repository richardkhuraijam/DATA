"""Driver GPS Simulator Producer.

Simulates a fleet of drivers sending location pings every 15-30s to Kafka topic driver_gps_pings.
Tracks driver status (AVAILABLE vs ON_TRIP) and zone locations.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import time
from typing import Any, Dict, List
import uuid

from dotenv import load_dotenv

from data.zones import NYCZones
from ingestion.producers.utils import create_producer, delivery_report

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("gps_simulator")


def generate_gps_ping(
    driver_id: int, zone_id: int, status: str, zones_helper: NYCZones
) -> Dict[str, Any]:
    lat, lon = zones_helper.get_centroid(zone_id)
    # Add minor GPS jitter (~100m)
    lat += random.uniform(-0.002, 0.002)
    lon += random.uniform(-0.002, 0.002)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "driver_id": driver_id,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "zone_id": zone_id,
        "status": status,
        "source": "gps_simulator",
    }


def main() -> None:
    topic = "driver_gps_pings"
    num_drivers = int(os.getenv("SIMULATOR_DRIVER_COUNT", "100"))
    producer = create_producer()
    zones_helper = NYCZones()

    # Initialize drivers across random valid TLC zones
    valid_zones = [z["zone_id"] for z in zones_helper.all_zones()]
    driver_states = [
        {
            "driver_id": 1000 + i,
            "zone_id": random.choice(valid_zones),
            "status": "AVAILABLE" if random.random() < 0.7 else "ON_TRIP",
        }
        for i in range(num_drivers)
    ]

    logger.info("Starting GPS simulator for %d drivers to topic '%s'...", num_drivers, topic)
    pings_sent = 0

    try:
        while True:
            for driver in driver_states:
                # 10% chance driver moves to an adjacent zone
                if random.random() < 0.1:
                    driver["zone_id"] = random.choice(valid_zones)

                # 5% chance driver toggles status
                if random.random() < 0.05:
                    driver["status"] = "ON_TRIP" if driver["status"] == "AVAILABLE" else "AVAILABLE"

                ping = generate_gps_ping(
                    driver["driver_id"], driver["zone_id"], driver["status"], zones_helper
                )
                key = str(ping["zone_id"])

                producer.produce(
                    topic=topic,
                    key=key,
                    value=json.dumps(ping),
                    callback=delivery_report,
                )
                pings_sent += 1

            producer.flush(timeout=1)
            logger.info("Sent batch of %d GPS pings (Total: %d)", num_drivers, pings_sent)
            time.sleep(15.0)
    except KeyboardInterrupt:
        logger.info("GPS simulator stopped by user.")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    main()
