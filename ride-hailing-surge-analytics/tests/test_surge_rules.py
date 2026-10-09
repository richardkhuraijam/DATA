import pytest

from processing.streaming.surge_rules import (
    calculate_surge_multiplier,
    compute_base_multiplier,
    compute_weather_adjustment,
    smooth_multiplier,
)


def test_base_multiplier_thresholds() -> None:
    assert compute_base_multiplier(0.5) == 1.0
    assert compute_base_multiplier(1.0) == 1.2
    assert compute_base_multiplier(1.4) == 1.2
    assert compute_base_multiplier(1.5) == 1.5
    assert compute_base_multiplier(2.0) == 2.0
    assert compute_base_multiplier(3.5) == 2.5


def test_weather_adjustment() -> None:
    assert compute_weather_adjustment(rain_mm_h=0.0, temp_c=20.0) == 0.0
    assert compute_weather_adjustment(rain_mm_h=3.0, temp_c=20.0) == 0.2
    assert compute_weather_adjustment(rain_mm_h=0.0, temp_c=2.0) == 0.1
    assert compute_weather_adjustment(rain_mm_h=3.0, temp_c=-1.0) == 0.3


def test_smoothing_and_cap() -> None:
    # Capped at 3.0x
    assert smooth_multiplier(raw_multiplier=3.5, previous_multiplier=None) == 3.0

    # Max change +0.3 per min
    assert smooth_multiplier(raw_multiplier=2.5, previous_multiplier=1.0) == 1.3
    assert smooth_multiplier(raw_multiplier=1.0, previous_multiplier=2.0) == 1.7


def test_full_surge_calculation() -> None:
    res = calculate_surge_multiplier(demand=20, supply=10, rain_mm_h=3.0, temp_c=20.0)
    # Ratio = 2.0 -> base = 2.0 -> weather +0.2 -> 2.2x
    assert res["ratio"] == 2.0
    assert res["base_multiplier"] == 2.0
    assert res["weather_adjustment"] == 0.2
    assert res["surge_multiplier"] == 2.2
