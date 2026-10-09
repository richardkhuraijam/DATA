from datetime import datetime

from data.validation import is_valid_trip, is_valid_zone_id, miles_to_km


def test_zone_id_bounds() -> None:
    assert is_valid_zone_id(1)
    assert is_valid_zone_id(263)
    assert not is_valid_zone_id(0)
    assert not is_valid_zone_id(264)
    assert not is_valid_zone_id(None)


def test_valid_completed_trip() -> None:
    row = {
        "pickup_zone_id": 79,
        "dropoff_zone_id": 132,
        "fare": 18.5,
        "pickup_time": datetime(2026, 1, 1, 8, 0),
        "dropoff_time": datetime(2026, 1, 1, 8, 22),
    }
    assert is_valid_trip(row)


def test_null_zone_invalid() -> None:
    assert not is_valid_trip({"pickup_zone_id": None, "dropoff_zone_id": 1, "fare": 1})
    assert not is_valid_trip({"pickup_zone_id": 1, "dropoff_zone_id": None, "fare": 1})


def test_negative_fare_invalid() -> None:
    assert not is_valid_trip(
        {
            "pickup_zone_id": 1,
            "dropoff_zone_id": 2,
            "fare": -0.01,
            "pickup_time": datetime(2026, 1, 1, 8, 0),
            "dropoff_time": datetime(2026, 1, 1, 8, 10),
        }
    )


def test_dropoff_before_pickup_invalid() -> None:
    assert not is_valid_trip(
        {
            "pickup_zone_id": 1,
            "dropoff_zone_id": 2,
            "fare": 10,
            "pickup_time": datetime(2026, 1, 1, 9, 0),
            "dropoff_time": datetime(2026, 1, 1, 8, 50),
        }
    )


def test_miles_to_km() -> None:
    assert miles_to_km(None) is None
    assert abs((miles_to_km(1) or 0) - 1.60934) < 1e-9
