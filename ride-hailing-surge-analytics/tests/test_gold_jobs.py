import pandas as pd

from processing.gold.driver_payout import compute as payout
from processing.gold.hourly_zone_agg import compute as hourly, dq_check


def trips(n_driver_trips=12, minutes=40):
    rows = []
    for i in range(n_driver_trips):
        req = pd.Timestamp("2026-10-12 09:00") + pd.Timedelta(minutes=50 * i)
        rows.append({"trip_id": f"a{i}", "driver_id": "D1", "pickup_zone_id": 1, "request_time": req, "pickup_time": req + pd.Timedelta(minutes=5),
                     "dropoff_time": req + pd.Timedelta(minutes=5 + minutes), "fare": 200.0, "distance_km": 10.0, "status": "COMPLETED"})
    rows.append({"trip_id": "c", "driver_id": "D2", "pickup_zone_id": 1, "request_time": pd.Timestamp("2026-10-12 10:00"), "pickup_time": pd.NaT,
                 "dropoff_time": pd.NaT, "fare": 0.0, "distance_km": 5.0, "status": "CANCELLED"})
    return pd.DataFrame(rows)


def test_hourly_agg_and_dq():
    out = hourly(trips())
    assert not dq_check(out)
    assert out["requests"].sum() == 13 and out["cancelled"].sum() == 1
    out.loc[0, "requests"] += 5
    assert dq_check(out)


def test_payout_rule_and_assumption():
    p = payout(trips(12, 40), "2026-10-12", online_hours=10)            # 12 trips * 40 min = 480 min of 600 -> 80% util
    d1 = p[p.driver_id == "D1"].iloc[0]
    assert round(d1.utilisation, 2) == 0.80 and d1.bonus_inr == 150.0
    p2 = payout(trips(11, 40), "2026-10-12", online_hours=10)             # only 11 trips -> no bonus
    assert p2[p2.driver_id == "D1"].iloc[0].bonus_inr == 0.0
    assert "D2" not in set(p.driver_id)                                    # cancelled trip earns nothing
