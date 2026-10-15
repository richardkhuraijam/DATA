"""Batch scoring (run every 30 min): latest feature row per zone -> predicted next-30-min demand -> Gold parquet."""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

from ml.features import build_features

ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger("score")


def score(trips_path: str, weather_path: str | None, model_path: str, out_path: str) -> pd.DataFrame:
    bundle = joblib.load(model_path)
    feats = bundle["features"]
    weather = pd.read_parquet(weather_path) if weather_path and Path(weather_path).exists() else None
    X = build_features(pd.read_parquet(trips_path), weather)
    latest = X.sort_values("bin").groupby("zone_id").tail(1).dropna(subset=feats)
    out = pd.DataFrame({"zone_id": latest["zone_id"].values, "as_of": latest["bin"].values,
                        "predicted_demand_next_30m": bundle["model"].predict(latest[feats]).clip(min=0).round(1),
                        "recent_lag_30m": latest["demand"].values, "model": bundle["name"],
                        "forecast_generated_at": datetime.now(timezone.utc).isoformat()})
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    logger.info("scored %d zones -> %s", len(out), out_path)
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--trips", default=str(ROOT / "data" / "raw" / "mysql" / "trips"))
    ap.add_argument("--weather", default=str(ROOT / "data" / "synthetic" / "weather_hourly.parquet"))
    ap.add_argument("--model", default=str(ROOT / "ml" / "model.joblib"))
    ap.add_argument("--out", default=str(ROOT / "data" / "lakehouse" / "gold_demand_forecast" / "forecast_latest.parquet"))
    a = ap.parse_args(); score(a.trips, a.weather, a.model, a.out)
