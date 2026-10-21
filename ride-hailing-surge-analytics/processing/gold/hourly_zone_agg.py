"""Hourly zone KPIs (Gold zone_hourly_kpis) with a data-quality gate. Pure pandas: no Spark needed.
Joins historical trips with the surge engine's Bronze output when it exists. Exit code 1 if DQ fails (Airflow marks task failed)."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger("hourly_zone_agg")


def compute(trips: pd.DataFrame, surge: pd.DataFrame | None = None) -> pd.DataFrame:
    t = trips.dropna(subset=["pickup_zone_id"]).copy()
    t["fare"] = pd.to_numeric(t["fare"], errors="coerce").fillna(0.0).astype(float)
    t["distance_km"] = pd.to_numeric(t["distance_km"], errors="coerce").fillna(0.0).astype(float)
    t["hour_ts"] = pd.to_datetime(t["request_time"]).dt.floor("h")
    t["eta_min"] = (pd.to_datetime(t["pickup_time"]) - pd.to_datetime(t["request_time"])).dt.total_seconds() / 60
    t["completed"] = (t["status"] == "COMPLETED").astype(int)
    t["cancelled"] = (t["status"] == "CANCELLED").astype(int)
    g = t.groupby(["pickup_zone_id", "hour_ts"]).agg(requests=("trip_id", "count"), completed=("completed", "sum"),
            cancelled=("cancelled", "sum"), avg_eta_min=("eta_min", "mean"), revenue_inr=("fare", "sum"),
            km=("distance_km", "sum")).reset_index()
    g["revenue_per_km"] = g["revenue_inr"] / g["km"].where(g["km"] > 0)
    if surge is not None and len(surge):
        s = surge.copy(); s["hour_ts"] = pd.to_datetime(s["window_end"]).dt.tz_localize(None).dt.floor("h")
        s = s.groupby(["zone_id", "hour_ts"]).agg(avg_demand=("demand", "mean"), avg_supply=("supply", "mean"),
                                                   avg_surge=("surge_multiplier", "mean"), max_surge=("surge_multiplier", "max")).reset_index()
        g = g.merge(s, left_on=["pickup_zone_id", "hour_ts"], right_on=["zone_id", "hour_ts"], how="left").drop(columns="zone_id")
    return g


def dq_check(df: pd.DataFrame) -> list[str]:
    errs = []
    if df["pickup_zone_id"].isna().any():
        errs.append("null zone")
    if ((df["completed"] + df["cancelled"]) != df["requests"]).any():
        errs.append("completed + cancelled != requests")
    if (df["requests"] <= 0).any():
        errs.append("non-positive request counts")
    return errs


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    trips = pd.read_parquet(ROOT / "data" / "raw" / "mysql" / "trips")
    sp = ROOT / "data" / "lakehouse" / "bronze_surge"
    surge = pd.read_parquet(sp) if sp.exists() else None
    out = compute(trips, surge)
    errs = dq_check(out)
    if errs:
        logger.error("DQ FAILED: %s", errs); sys.exit(1)
    dest = ROOT / "data" / "gold" / "zone_hourly_kpis.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True); out.to_parquet(dest, index=False)
    logger.info("zone_hourly_kpis: %d rows -> %s", len(out), dest)


if __name__ == "__main__":
    main()
