"""Pure validation helpers for trip rows (unit-tested)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

NYC_ZONE_ID_MIN = 1
NYC_ZONE_ID_MAX = 263
MILES_TO_KM = 1.60934

COMPLETED = "COMPLETED"
CANCELLED = "CANCELLED"
VALID_STATUSES = frozenset({COMPLETED, CANCELLED})
CANCEL_REASONS = ("RIDER_CANCEL", "DRIVER_CANCEL", "TIMEOUT", "NO_SHOW")


def is_valid_zone_id(zone_id: Any) -> bool:
    if zone_id is None:
        return False
    try:
        value = int(zone_id)
    except (TypeError, ValueError):
        return False
    return NYC_ZONE_ID_MIN <= value <= NYC_ZONE_ID_MAX


def is_valid_trip(row: Mapping[str, Any]) -> bool:
    """Return False for null zones, negative fares, or dropoff before pickup."""
    if not is_valid_zone_id(row.get("pickup_zone_id")):
        return False
    if not is_valid_zone_id(row.get("dropoff_zone_id")):
        return False
    fare = row.get("fare")
    if fare is not None:
        try:
            if float(fare) < 0:
                return False
        except (TypeError, ValueError):
            return False
    pickup = row.get("pickup_time")
    dropoff = row.get("dropoff_time")
    if pickup is not None and dropoff is not None:
        if isinstance(pickup, datetime) and isinstance(dropoff, datetime):
            if dropoff < pickup:
                return False
    return True


def miles_to_km(miles: Any) -> float | None:
    if miles is None:
        return None
    try:
        return float(miles) * MILES_TO_KM
    except (TypeError, ValueError):
        return None
