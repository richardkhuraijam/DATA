from pathlib import Path

import pytest

from data.zones import load_city_zones

LOOKUP = Path("data/raw/zones/taxi_zone_lookup.csv")


@pytest.mark.skipif(not LOOKUP.exists(), reason="Run make download first")
def test_tlc_zone_count_is_263() -> None:
    zones = load_city_zones()
    assert len(zones) == 263
    ids = {z["zone_id"] for z in zones}
    assert ids == set(range(1, 264))
    east_village = next(z for z in zones if z["zone_id"] == 79)
    assert east_village["borough"] == "Manhattan"
    if east_village["centroid_lat"] is not None:
        assert 40.4 < east_village["centroid_lat"] < 41.0
        assert -74.3 < east_village["centroid_lon"] < -73.6
