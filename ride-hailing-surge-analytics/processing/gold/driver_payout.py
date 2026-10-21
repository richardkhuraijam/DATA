"""Daily driver incentive payout + HTML/CSV report (INR).

Rule (from the proposal): utilisation >= 78% AND >= 12 completed trips in the day -> bonus; plus an extra amount per
trip started in a surge window. NOTE: historical trips have no driver online-time source, so utilisation is
on-trip minutes / (ONLINE_HOURS_PER_DAY * 60) with ONLINE_HOURS_PER_DAY an explicit, configurable ASSUMPTION (default 10).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BONUS_INR, SURGE_EXTRA_INR, UTIL_MIN, TRIPS_MIN = 150.0, 10.0, 0.78, 12


def compute(trips: pd.DataFrame, date: str, online_hours: float, surge_hours: set | None = None) -> pd.DataFrame:
    t = trips[(pd.to_datetime(trips["request_time"]).dt.date.astype(str) == date) & (trips["status"] == "COMPLETED")].copy()
    t["on_trip_min"] = (pd.to_datetime(t["dropoff_time"]) - pd.to_datetime(t["pickup_time"])).dt.total_seconds() / 60
    t["in_surge"] = t["request_time"].dt.floor("h").isin(surge_hours or set()) if surge_hours else False
    g = t.groupby("driver_id").agg(trips=("trip_id", "count"), revenue_inr=("fare", "sum"), on_trip_min=("on_trip_min", "sum"),
                                   surge_trips=("in_surge", "sum")).reset_index()
    g["utilisation"] = (g["on_trip_min"] / (online_hours * 60)).clip(upper=1.0)
    g["bonus_inr"] = ((g["utilisation"] >= UTIL_MIN) & (g["trips"] >= TRIPS_MIN)) * BONUS_INR
    g["surge_pay_inr"] = g["surge_trips"] * SURGE_EXTRA_INR
    g["total_incentive_inr"] = g["bonus_inr"] + g["surge_pay_inr"]
    return g.sort_values("total_incentive_inr", ascending=False)


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--date", required=True); ap.add_argument("--out", default=str(ROOT / "data" / "gold" / "payouts"))
    a = ap.parse_args()
    trips = pd.read_parquet(ROOT / "data" / "raw" / "mysql" / "trips")
    pay = compute(trips, a.date, float(os.getenv("ONLINE_HOURS_PER_DAY", "10")))
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    pay.to_csv(out / f"payout_{a.date}.csv", index=False)
    html = (f"<h2>Driver incentive payout {a.date}</h2><p>Drivers with trips: {len(pay)} | Bonus earners: {(pay.bonus_inr > 0).sum()} | "
            f"Total incentive: INR {pay.total_incentive_inr.sum():,.0f}</p>" + pay.head(25).round(2).to_html(index=False))
    (out / f"payout_{a.date}.html").write_text(html)
    print(f"{len(pay)} drivers, {(pay.bonus_inr > 0).sum()} bonus earners -> {out}")


if __name__ == "__main__":
    main()
