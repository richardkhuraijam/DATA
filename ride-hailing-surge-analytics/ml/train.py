"""ML Model Training Script for 30-Min Zone Demand Forecasting.

Trains Linear Regression baseline + Random Forest Regressor using scikit-learn & MLflow.
Enforces time-based split (NO shuffling).
Acceptance Criteria: R2 >= 0.70 on held-out test set.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv
import mlflow
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from ml.features import generate_synthetic_feature_dataset, prepare_features_and_target

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("train_model")


def train_and_evaluate_models() -> Dict[str, Any]:
    logger.info("Generating feature dataset...")
    df = generate_synthetic_feature_dataset(num_records=5000)

    # Time-based split: First 80% train, Last 20% test (NO SHUFFLING)
    train_size = int(len(df) * 0.8)
    train_df = df.iloc[:train_size]
    test_df = df.iloc[train_size:]

    X_train, y_train = prepare_features_and_target(train_df)
    X_test, y_test = prepare_features_and_target(test_df)

    logger.info("Train set size: %d, Test set size: %d", len(X_train), len(X_test))

    mlflow.set_experiment("RideHail_Demand_Forecast_30m")

    results = {}

    with mlflow.start_run(run_name="Demand_Forecast_Training"):
        # 1. Baseline: Linear Regression
        lr = LinearRegression()
        lr.fit(X_train, y_train)
        lr_pred = lr.predict(X_test)
        lr_r2 = r2_score(y_test, lr_pred)
        lr_mae = mean_absolute_error(y_test, lr_pred)
        lr_rmse = np.sqrt(mean_squared_error(y_test, lr_pred))

        logger.info("Linear Regression Baseline — R2: %.4f, MAE: %.2f, RMSE: %.2f", lr_r2, lr_mae, lr_rmse)

        # 2. Main Model: Random Forest Regressor
        rf = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42)
        rf.fit(X_train, y_train)
        rf_pred = rf.predict(X_test)
        rf_r2 = r2_score(y_test, rf_pred)
        rf_mae = mean_absolute_error(y_test, rf_pred)
        rf_rmse = np.sqrt(mean_squared_error(y_test, rf_pred))

        logger.info("Random Forest Model — R2: %.4f, MAE: %.2f, RMSE: %.2f", rf_r2, rf_mae, rf_rmse)

        mlflow.log_params({"n_estimators": 100, "max_depth": 12, "split": "time_based_no_shuffle"})
        mlflow.log_metrics({"baseline_r2": lr_r2, "rf_r2": rf_r2, "rf_mae": rf_mae, "rf_rmse": rf_rmse})

        target_met = rf_r2 >= 0.70
        logger.info("Model Acceptance Criteria (R2 >= 0.70): %s (Achieved R2 = %.4f)", "PASSED" if target_met else "FAILED", rf_r2)

        results = {
            "baseline_r2": round(lr_r2, 4),
            "rf_r2": round(rf_r2, 4),
            "rf_mae": round(rf_mae, 2),
            "rf_rmse": round(rf_rmse, 2),
            "target_met": target_met,
            "model": rf,
        }

    return results


def main() -> None:
    results = train_and_evaluate_models()
    print("\n--- ML MODEL EVALUATION REPORT ---")
    print(f"Linear Regression Baseline R2: {results['baseline_r2']}")
    print(f"Random Forest Model R2: {results['rf_r2']}")
    print(f"Random Forest MAE: {results['rf_mae']}")
    print(f"Random Forest RMSE: {results['rf_rmse']}")
    print(f"Model SLA (R2 >= 0.70): {'PASSED' if results['target_met'] else 'FAILED'}")


if __name__ == "__main__":
    main()
