"""Open-Meteo Weather Producer.

Polls Open-Meteo API for NYC weather (temperature and precipitation) every 60s
and publishes events to Kafka weather topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict
import uuid

from dotenv import load_dotenv
import requests

from ingestion.producers.utils import create_producer, delivery_report

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("weather")


def fetch_open_meteo_weather() -> Dict[str, Any]:
    lat = float(os.getenv("OPEN_METEO_LAT", "40.7580"))
    lon = float(os.getenv("OPEN_METEO_LON", "-73.9855"))
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&current_weather=true&hourly=precipitation,rain"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        current = data.get("current_weather", {})

        # Extract current rain from hourly forecast if available
        hourly = data.get("hourly", {})
        rain_list = hourly.get("rain", [0.0])
        current_rain = float(rain_list[0]) if rain_list else 0.0

        return {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "city": "NYC",
            "temperature_c": float(current.get("temperature", 20.0)),
            "precipitation_mm_h": current_rain,
            "rain_mm_h": current_rain,
            "weather_code": int(current.get("weathercode", 0)),
            "source": "open_meteo",
        }
    except Exception as exc:
        logger.warning("Error fetching Open-Meteo weather API: %s. Using default baseline.", exc)
        return {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "city": "NYC",
            "temperature_c": 20.0,
            "precipitation_mm_h": 0.0,
            "rain_mm_h": 0.0,
            "weather_code": 0,
            "source": "open_meteo_fallback",
        }


def main() -> None:
    topic = "weather"
    producer = create_producer()
    logger.info("Starting Open-Meteo weather producer polling every 60s...")

    try:
        while True:
            weather_event = fetch_open_meteo_weather()
            producer.produce(
                topic=topic,
                key="NYC",
                value=json.dumps(weather_event),
                callback=delivery_report,
            )
            producer.flush(timeout=2)
            logger.info(
                "Published weather event: Temp=%.1f C, Rain=%.2f mm/h",
                weather_event["temperature_c"],
                weather_event["rain_mm_h"],
            )
            time.sleep(60.0)
    except KeyboardInterrupt:
        logger.info("Weather producer stopped by user.")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    main()
