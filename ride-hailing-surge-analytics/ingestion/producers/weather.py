"""Open-Meteo CURRENT weather for Hyderabad, every 60 s (no API key).
Fixes the first version, which read hourly[0] (= midnight) instead of the current conditions."""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone

import requests

from data.hyderabad import WEATHER_LAT, WEATHER_LON
from ingestion.producers.utils import create_producer, send

logger = logging.getLogger("weather")
URL = (f"https://api.open-meteo.com/v1/forecast?latitude={WEATHER_LAT}&longitude={WEATHER_LON}"
       "&current=temperature_2m,precipitation,rain,weather_code")


def fetch_weather() -> dict:
    cur = requests.get(URL, timeout=15).json()["current"]
    return {"event_id": uuid.uuid4().hex, "event_time": datetime.now(timezone.utc).isoformat(), "city": "HYDERABAD",
            "temperature_c": float(cur["temperature_2m"]), "rain_mm_h": float(cur.get("rain") or 0.0),
            "precipitation_mm_h": float(cur.get("precipitation") or 0.0), "weather_code": int(cur.get("weather_code") or 0),
            "source": "open_meteo"}


def main() -> None:
    producer, backoff = create_producer(), 5
    while True:
        try:
            ev = fetch_weather()
            send(producer, "weather", "HYDERABAD", ev); producer.flush(5)
            logger.info("weather: %.1f C, rain %.2f mm/h", ev["temperature_c"], ev["rain_mm_h"])
            backoff = 5; time.sleep(60)
        except Exception as exc:  # noqa: BLE001  - API outage must not kill the producer
            logger.warning("weather poll failed (%s); retry in %ss", exc, backoff)
            time.sleep(backoff); backoff = min(backoff * 2, 120)


if __name__ == "__main__":
    main()
