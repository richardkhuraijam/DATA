"""Feature Engineering for 30-Minute Zone Demand Forecasting Model.

Target: trip_requests_next_30m (number of trip requests in a zone in the next 30 minutes).

Features:
- zone_id, hour, day_of_week, is_weekend, is_holiday
- Demand Lags: lag_30m, lag_60m, lag_24h, lag_7d
- Rolling Means: rolling_mean_1h, rolling_mean_2h
- Weather: rain_mm_h, temperature_c
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def generate_synthetic_feature_dataset(num_records: int = 5000) -> pd.DataFrame:
    np.random.seed(42)

    zone_ids = np.random.randint(1, 264, size=num_records)
    hours = np.random.randint(0, 24, size=num_records)
    day_of_week = np.random.randint(0, 7, size=num_records)
    is_weekend = (day_of_week >= 5).astype(int)

    # Base demand pattern by hour (morning/evening peak)
    hour_factor = np.sin((hours - 6) / 24 * 2 * np.pi) + 1.5
    base_demand = (zone_ids % 10 + 1) * 10 * hour_factor

    # Lag features
    lag_30m = np.maximum(0, base_demand + np.random.normal(0, 5, size=num_records))
    lag_60m = np.maximum(0, base_demand + np.random.normal(0, 8, size=num_records))
    lag_24h = np.maximum(0, base_demand + np.random.normal(0, 10, size=num_records))
    lag_7d = np.maximum(0, base_demand + np.random.normal(0, 12, size=num_records))

    rolling_mean_1h = (lag_30m + lag_60m) / 2.0
    rolling_mean_2h = (lag_30m + lag_60m + lag_24h) / 3.0

    rain_mm_h = np.random.choice([0.0, 0.5, 2.8, 5.0], size=num_records, p=[0.7, 0.15, 0.1, 0.05])
    temperature_c = np.random.normal(18, 8, size=num_records)

    # Target: 30-min forward demand
    target = (
        0.4 * lag_30m
        + 0.3 * lag_60m
        + 0.2 * lag_24h
        + 0.1 * lag_7d
        + 5.0 * rain_mm_h
        + np.random.normal(0, 3, size=num_records)
    )
    target = np.maximum(0, target)

    df = pd.DataFrame(
        {
            "zone_id": zone_ids,
            "hour": hours,
            "day_of_week": day_of_week,
            "is_weekend": is_weekend,
            "is_holiday": 0,
            "lag_30m": lag_30m,
            "lag_60m": lag_60m,
            "lag_24h": lag_24h,
            "lag_7d": lag_7d,
            "rolling_mean_1h": rolling_mean_1h,
            "rolling_mean_2h": rolling_mean_2h,
            "rain_mm_h": rain_mm_h,
            "temperature_c": temperature_c,
            "target_demand_next_30m": target,
        }
    )
    return df


def prepare_features_and_target(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    feature_cols = [
        "zone_id",
        "hour",
        "day_of_week",
        "is_weekend",
        "is_holiday",
        "lag_30m",
        "lag_60m",
        "lag_24h",
        "lag_7d",
        "rolling_mean_1h",
        "rolling_mean_2h",
        "rain_mm_h",
        "temperature_c",
    ]
    X = df[feature_cols]
    y = df["target_demand_next_30m"]
    return X, y
