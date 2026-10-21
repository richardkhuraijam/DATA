"""Feature engineering for the 30-minute zone demand forecast, built from REAL trip history (no random target).

Row = (zone, 30-min bin t). Target y_next = requests in the NEXT bin. Every feature at t only uses information
available at t (no leakage): current/lagged demand, same-time-yesterday / last-week demand for the TARGET bin, rolling
means, hour/day, weekend/holiday, and hourly weather (rain, temperature) joined from the weather table.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

BIN = "30min"
FEATURES = ["zone_id", "hour", "dow", "is_weekend", "is_holiday", "demand", "lag_30m", "lag_60m", "lag_24h", "lag_7d",
            "roll_mean_1h", "roll_mean_2h", "rain_mm_h", "temperature_c"]


def build_features(trips: pd.DataFrame, weather: Optional[pd.DataFrame] = None, holidays: Optional[set] = None) -> pd.DataFrame:
    t = trips.dropna(subset=["pickup_zone_id", "request_time"]).copy()
    t["bin"] = pd.to_datetime(t["request_time"]).dt.floor(BIN)
    counts = t.groupby(["pickup_zone_id", "bin"]).size().rename("demand").reset_index()
    full = pd.date_range(counts["bin"].min(), counts["bin"].max(), freq=BIN)
    frames = []
    for zone, g in counts.groupby("pickup_zone_id"):
        s = g.set_index("bin")["demand"].reindex(full, fill_value=0).astype(float)
        f = pd.DataFrame({"zone_id": int(zone), "bin": full, "demand": s.values})
        f["y_next"] = f["demand"].shift(-1)
        f["lag_30m"], f["lag_60m"] = f["demand"].shift(1), f["demand"].shift(2)
        f["lag_24h"] = f["demand"].shift(47)           # demand in the target bin, one day earlier
        f["lag_7d"] = f["demand"].shift(335)           # ... one week earlier
        f["roll_mean_1h"], f["roll_mean_2h"] = f["demand"].rolling(2).mean(), f["demand"].rolling(4).mean()
        frames.append(f)
    X = pd.concat(frames, ignore_index=True)
    X["hour"], X["dow"] = X["bin"].dt.hour, X["bin"].dt.dayofweek
    X["is_weekend"] = (X["dow"] >= 5).astype(int)
    X["is_holiday"] = X["bin"].dt.date.isin(holidays or set()).astype(int)
    if weather is not None and len(weather):
        w = weather.rename(columns={"hour_ts": "hour_key", "rain_mm": "rain_mm_h", "temp_c": "temperature_c"})
        X["hour_key"] = X["bin"].dt.floor("h")
        X = X.merge(w[["hour_key", "rain_mm_h", "temperature_c"]], on="hour_key", how="left").drop(columns="hour_key")
        X["rain_mm_h"] = X["rain_mm_h"].fillna(0.0)
        X["temperature_c"] = X["temperature_c"].fillna(X["temperature_c"].median())
    else:
        X["rain_mm_h"], X["temperature_c"] = 0.0, 28.0
    return X
