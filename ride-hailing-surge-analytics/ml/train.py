"""Train the 30-min demand forecast: Linear Regression baseline + Random Forest, TIME-based split (no shuffling),
MLflow tracking (if installed). Reports R2 / MAE / RMSE honestly - whatever they are.

  python -m ml.train --trips data/raw/mysql/trips --weather data/synthetic/weather_hourly.parquet

NOTE: on this project's SYNTHETIC history the score reflects the synthetic demand process, not real Hyderabad riders.
Say so in your report.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from data.validation import is_valid_trip
from ml.features import FEATURES, build_features

logger = logging.getLogger("train_model")
ROOT = Path(__file__).resolve().parents[1]


def load_trips(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    keep = df.apply(lambda r: is_valid_trip(r.to_dict()), axis=1) if len(df) < 200_000 else slice(None)
    return df[keep]


def train(trips_path: str, weather_path: str | None, out: str) -> dict:
    trips = load_trips(trips_path)
    weather = pd.read_parquet(weather_path) if weather_path and Path(weather_path).exists() else None
    X = build_features(trips, weather).dropna(subset=FEATURES + ["y_next"]).sort_values("bin")
    cut = X["bin"].quantile(0.8)
    tr, te = X[X["bin"] <= cut], X[X["bin"] > cut]
    logger.info("train=%d test=%d (time split at %s)", len(tr), len(te), cut)
    try:
        import mlflow
        mlflow.set_experiment("RideHail_Hyderabad_Demand_Forecast_30m")
    except ImportError:
        mlflow = None
    results, best = {}, None
    for name, model in (("linear_regression", LinearRegression()),
                        ("random_forest", RandomForestRegressor(n_estimators=150, max_depth=16, min_samples_leaf=3, n_jobs=-1, random_state=42))):
        model.fit(tr[FEATURES], tr["y_next"]); pred = model.predict(te[FEATURES])
        m = {"r2": float(r2_score(te["y_next"], pred)), "mae": float(mean_absolute_error(te["y_next"], pred)),
             "rmse": float(np.sqrt(mean_squared_error(te["y_next"], pred)))}
        results[name] = m
        logger.info("%s: %s", name, {k: round(v, 4) for k, v in m.items()})
        if mlflow:
            with mlflow.start_run(run_name=name):
                mlflow.log_params({"model": name, "train_rows": len(tr), "test_rows": len(te)}); mlflow.log_metrics(m)
        if best is None or m["r2"] > best[0]:
            best = (m["r2"], name, model)
    joblib.dump({"model": best[2], "features": FEATURES, "name": best[1], "metrics": results}, out)
    results["best"], results["target_met"] = best[1], bool(best[0] >= 0.70)
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--trips", default=str(ROOT / "data" / "raw" / "mysql" / "trips"))
    ap.add_argument("--weather", default=str(ROOT / "data" / "synthetic" / "weather_hourly.parquet"))
    ap.add_argument("--out", default=str(ROOT / "ml" / "model.joblib"))
    a = ap.parse_args()
    r = train(a.trips, a.weather, a.out)
    print("\n--- DEMAND FORECAST EVALUATION (held-out last 20% of time) ---")
    for k in ("linear_regression", "random_forest"):
        print(f"{k:18s} R2={r[k]['r2']:.4f}  MAE={r[k]['mae']:.2f}  RMSE={r[k]['rmse']:.2f}")
    print(f"Best: {r['best']} | Model SLA (R2 >= 0.70): {'PASSED' if r['target_met'] else 'FAILED'}")


if __name__ == "__main__":
    main()
