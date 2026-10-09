"""Surge Multiplier Rules Engine.

Calculates base surge multiplier from demand/supply ratio, applies weather adjustments,
smooths rate changes (+/-0.3x per min), and caps the multiplier at 3.0x.

Rule Table:
- ratio < 1.0       -> 1.0x
- 1.0 <= ratio < 1.5 -> 1.2x
- 1.5 <= ratio < 2.0 -> 1.5x
- 2.0 <= ratio < 3.0 -> 2.0x
- ratio >= 3.0      -> 2.5x

Weather Adjustments:
- +0.2x if rain >= 2.5 mm/h
- +0.1x if temp < 5.0 C or temp > 35.0 C

Cap & Smoothing:
- Cap at 3.0x
- Max change per minute: +/-0.3x
"""

from __future__ import annotations

from typing import Dict, Optional


def compute_base_multiplier(ratio: float) -> float:
    if ratio < 1.0:
        return 1.0
    elif ratio < 1.5:
        return 1.2
    elif ratio < 2.0:
        return 1.5
    elif ratio < 3.0:
        return 2.0
    else:
        return 2.5


def compute_weather_adjustment(rain_mm_h: float, temp_c: float) -> float:
    adj = 0.0
    if rain_mm_h >= 2.5:
        adj += 0.2
    if temp_c < 5.0 or temp_c > 35.0:
        adj += 0.1
    return round(adj, 2)


def smooth_multiplier(
    raw_multiplier: float,
    previous_multiplier: Optional[float] = None,
    max_change_per_min: float = 0.3,
    max_cap: float = 3.0,
) -> float:
    """Apply capping (3.0x max) and smoothing (+/- max_change_per_min)."""
    capped = min(raw_multiplier, max_cap)
    if previous_multiplier is None:
        return round(capped, 2)

    diff = capped - previous_multiplier
    if abs(diff) > max_change_per_min:
        if diff > 0:
            smoothed = previous_multiplier + max_change_per_min
        else:
            smoothed = previous_multiplier - max_change_per_min
    else:
        smoothed = capped

    return round(min(smoothed, max_cap), 2)


def calculate_surge_multiplier(
    demand: int,
    supply: int,
    rain_mm_h: float = 0.0,
    temp_c: float = 20.0,
    previous_multiplier: Optional[float] = None,
) -> Dict[str, float]:
    effective_supply = max(supply, 1)
    ratio = demand / float(effective_supply)
    base = compute_base_multiplier(ratio)
    weather_adj = compute_weather_adjustment(rain_mm_h, temp_c)
    raw = base + weather_adj
    final_mult = smooth_multiplier(raw, previous_multiplier)

    return {
        "demand": float(demand),
        "supply": float(supply),
        "ratio": round(ratio, 3),
        "base_multiplier": base,
        "weather_adjustment": weather_adj,
        "surge_multiplier": final_mult,
    }
