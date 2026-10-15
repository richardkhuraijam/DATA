import random
from datetime import datetime, timezone

from data.hyderabad import (LAT_LON_BOUNDS, ZONE_BY_ID, ZONES, demand_rate_per_min, generate_history, hour_profile,
                            is_inside_city, nearest_zone_id)
from ingestion.producers.demand_stream import make_event, poisson
from ingestion.producers.gps_simulator import make_ping, zone_weights
from storage.hbase_schema import format_hbase_row_key


def test_zones_are_valid_and_in_hyderabad() -> None:
    assert len(ZONES) >= 35 and [z.zone_id for z in ZONES] == list(range(1, len(ZONES) + 1))
    assert all(is_inside_city(z.lat, z.lon) for z in ZONES)
    assert len({z.name for z in ZONES}) == len(ZONES)


def test_nearest_zone() -> None:
    hitech = next(z for z in ZONES if z.name == "Hitech City")
    assert nearest_zone_id(hitech.lat + 0.001, hitech.lon - 0.001) == hitech.zone_id


def test_it_hub_peaks_in_evening_and_dips_on_weekends() -> None:
    assert hour_profile("it_hub", 19, False) > 2 * hour_profile("it_hub", 3, False)
    assert hour_profile("it_hub", 19, True) < hour_profile("it_hub", 19, False)
    assert hour_profile("airport", 22.5, False) > hour_profile("airport", 11, False)


def test_rain_raises_demand() -> None:
    z = ZONES[0]
    t = datetime(2026, 10, 12, 19, 0)
    assert demand_rate_per_min(z, t, 1.0, rain_mm=5) > demand_rate_per_min(z, t, 1.0, rain_mm=0)


def test_history_generator_shape() -> None:
    trips, weather = generate_history(3000, days=5, seed=3)
    assert 2500 < len(trips) < 3500 and len(weather) == 5 * 24
    assert all(t["pickup_time"] > t["request_time"] and t["dropoff_time"] > t["pickup_time"] for t in trips)
    assert all(0.5 < t["distance_km"] < 60 and t["fare"] > 0 for t in trips)
    by_hour = [0] * 24
    for t in trips:
        by_hour[t["request_time"].hour] += 1
    assert max(by_hour[7:10] + by_hour[17:21]) > 2 * min(by_hour[2:5])         # peaks vs small hours


def test_demand_event_format() -> None:
    now = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
    ev = make_event(1, now, random.Random(1))
    assert ev["zone_id"] == 1 and ev["dropoff_zone_id"] in ZONE_BY_ID and ev["dropoff_zone_id"] != 1
    assert ev["event_time"].startswith("2026-10-12T12:00:00") and len(ev["event_id"]) == 32


def test_ping_format_and_location_inside_hyderabad() -> None:
    now = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
    p = make_ping("DRV-0001", 3, "AVAILABLE", random.Random(2), now)
    assert p["status"] == "AVAILABLE" and p["zone_id"] == 3 and is_inside_city(p["lat"], p["lon"])
    assert len(zone_weights(datetime(2026, 10, 12, 19, 0))) == len(ZONES)


def test_poisson_mean() -> None:
    rng = random.Random(0)
    xs = [poisson(rng, 3.0) for _ in range(4000)]
    assert 2.8 < sum(xs) / len(xs) < 3.2 and poisson(rng, 0) == 0


def test_hbase_row_key_zero_padded_and_sortable() -> None:
    assert format_hbase_row_key(7, "2026-10-12T14:05:00Z") == "007#202610121405"
    assert format_hbase_row_key(7, "2026-10-12T14:05:00Z") < format_hbase_row_key(12, "2026-10-12T14:05:00Z")
