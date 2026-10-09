from datetime import datetime, timezone
import pytest

from data.zones import NYCZones
from ingestion.producers.citybikes import format_citybikes_event
from ingestion.producers.gps_simulator import generate_gps_ping
from ingestion.producers.tlc_replay import format_trip_request_event
from ingestion.producers.weather import fetch_open_meteo_weather


def test_format_trip_request_event() -> None:
    row = {"PULocationID": 132, "DOLocationID": 230, "trip_miles": 5.2, "base_passenger_fare": 22.5}
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    event = format_trip_request_event(row, current_time=now)

    assert event["zone_id"] == 132
    assert event["dropoff_zone_id"] == 230
    assert event["distance_miles"] == 5.2
    assert event["fare"] == 22.5
    assert event["event_time"] == "2026-01-01T12:00:00+00:00"
    assert "event_id" in event


def test_generate_gps_ping() -> None:
    zones_helper = NYCZones()
    ping = generate_gps_ping(driver_id=501, zone_id=79, status="AVAILABLE", zones_helper=zones_helper)

    assert ping["driver_id"] == 501
    assert ping["zone_id"] == 79
    assert ping["status"] == "AVAILABLE"
    assert 40.0 <= ping["lat"] <= 41.5
    assert -74.5 <= ping["lon"] <= -73.0


def test_format_citybikes_event() -> None:
    zones_helper = NYCZones()
    # Station near Midtown NYC (Zone 161 / Midtown Center approx)
    station = {"id": "stat-123", "latitude": 40.755, "longitude": -73.98, "free_bikes": 10, "empty_slots": 5}
    event = format_citybikes_event(station, zones_helper)

    assert event is not None
    assert event["station_id"] == "stat-123"
    assert event["free_bikes"] == 10
    assert isinstance(event["zone_id"], int)


def test_fetch_weather_structure() -> None:
    event = fetch_open_meteo_weather()
    assert "temperature_c" in event
    assert "rain_mm_h" in event
    assert event["city"] == "NYC"
