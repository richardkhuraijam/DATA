"""Batch Scorer for 30-Minute Demand Forecast.

Scores all 263 TLC zones every 30 minutes and writes predictions to Gold demand_forecast table.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd

from ml.features import generate_synthetic_feature_dataset, prepare_features_and_target
from ml.train import train_and_evaluate_models

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("score_model")


def batch_score_demand_forecast() -> pd.DataFrame:
    logger.info("Running batch demand forecast scorer...")
    training_res = train_and_evaluate_models()
    model = training_res["model"]

    # Generate current feature snapshot for all 263 zones
    current_df = generate_synthetic_feature_dataset(num_records=263)
    X_current, _ = prepare_features_and_target(current_df)

    predictions = model.predict(X_current)
    current_df["predicted_demand_next_30m"] = predictions.round(1)
    current_df["forecast_generated_at"] = datetime.now(timezone.utc).isoformat()

    gold_forecast_dir = ROOT / "data" / "lakehouse" / "gold_demand_forecast"
    os.makedirs(gold_forecast_dir, exist_ok=True)
    output_path = gold_forecast_dir / "forecast_latest.parquet"

    current_df.to_parquet(output_path, index=False)
    logger.info("Successfully wrote 263 zone demand forecasts to %s", output_path)

    return current_df


def main() -> None:
    df = batch_score_demand_forecast()
    print(df[["zone_id", "hour", "predicted_demand_next_30m"]].head(10))


if __name__ == "__main__":
    main()
