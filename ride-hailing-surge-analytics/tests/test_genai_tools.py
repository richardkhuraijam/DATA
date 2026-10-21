import os
from datetime import datetime, timedelta

import pytest

os.environ["SURGE_STORE"] = "memory"
from genai import tools
from storage.hbase_schema import _MEM


def test_unknown_zone_and_no_data_never_invent_numbers():
    _MEM.rows.clear()
    assert "error" in tools.get_current_surge("Atlantis")
    assert "error" in tools.get_current_surge("Hitech City")          # known zone, no data yet -> error, not fake numbers
    assert "error" in tools.top_zones("demand")


def test_current_surge_and_top_zones_from_store():
    _MEM.rows.clear()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z")
    _MEM.put_surge(1, now, 2.0, 40, 10, 4.0)
    _MEM.put_surge(2, now, 1.2, 10, 10, 1.0)
    r = tools.get_current_surge("hitech")
    assert r["zone_id"] == 1 and float(r["multiplier"]) == 2.0
    top = tools.top_zones("surge_multiplier", 2)
    assert [z["zone_id"] for z in top] == [1, 2]
    assert "error" in tools.top_zones("bogus_metric")


def test_zone_resolution():
    assert tools.resolve_zone("Banjara Hills") and tools.resolve_zone("5") == 5 and tools.resolve_zone("zzz") is None
