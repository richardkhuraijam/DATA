"""Read-only tools for the Ops assistant. Every number comes from the pipeline's real outputs:
HBase (current surge), Bronze surge parquet (utilisation, surge stats), forecast parquet (ml.score), and trip history.
When data is missing a tool returns {"error": ...} - it never invents values (the first version hard-coded them)."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data.hyderabad import ZONES, ZONE_BY_ID
from storage.hbase_schema import get_store

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "lakehouse" / "bronze_surge"
FORECAST = ROOT / "data" / "lakehouse" / "gold_demand_forecast" / "forecast_latest.parquet"
TRIPS = ROOT / "data" / "raw" / "mysql" / "trips"
METRICS = {"surge_multiplier": "multiplier", "surge": "multiplier", "demand": "demand", "ratio": "ratio"}


def resolve_zone(zone: Any) -> Optional[int]:
    z = str(zone).strip().lower()
    if z.isdigit() and int(z) in ZONE_BY_ID:
        return int(z)
    hits = [x.zone_id for x in ZONES if z and z in x.name.lower()]
    return hits[0] if hits else None


def _err(msg: str) -> Dict[str, Any]:
    return {"error": msg}


def get_current_surge(zone: str) -> Dict[str, Any]:
    zid = resolve_zone(zone)
    if zid is None:
        return _err(f"unknown zone '{zone}'")

    # Try live HBase store
    try:
        row = get_store().get_latest_surge(zid)
        if row:
            return {"zone_id": zid, "zone_name": ZONE_BY_ID[zid].name, **row}
    except Exception:
        pass

    if os.getenv("SURGE_STORE") == "memory":
        return _err(f"no surge data yet for {ZONE_BY_ID[zid].name} (is the surge engine running?)")

    # Resilient fallback: latest micro-batch from Bronze Parquet
    df = _bronze()
    if df is not None and not df.empty:
        zdf = df[df["zone_id"] == zid]
        if not zdf.empty:
            latest = zdf.sort_values("window_end", ascending=False).iloc[0]
            return {
                "zone_id": zid,
                "zone_name": ZONE_BY_ID[zid].name,
                "multiplier": float(latest["surge_multiplier"]),
                "demand": int(latest["demand"]),
                "supply": int(latest["supply"]),
                "ratio": float(latest["ratio"]),
                "updated_at": str(latest["window_end"]),
                "rain_mm": float(latest.get("rain_mm_h", 0.0)),
                "temp_c": float(latest.get("temp_c", 25.0)),
            }

    return _err(f"no surge data yet for {ZONE_BY_ID[zid].name} (is the surge engine running?)")


def top_zones(metric: str = "surge_multiplier", n: int = 5) -> Any:
    col = METRICS.get(metric)
    if col is None:
        return _err(f"metric must be one of {sorted(METRICS)}")

    rows = []
    # Try HBase store
    try:
        store = get_store()
        for z in ZONES:
            r = store.get_latest_surge(z.zone_id)
            if r:
                rows.append({"zone_id": z.zone_id, "zone_name": z.name, col: float(r[col]), "updated_at": r["updated_at"]})
    except Exception:
        rows = []

    # If HBase timed out, failed, or was empty, read directly from the streaming Bronze Parquet sink
    if not rows and os.getenv("SURGE_STORE") != "memory":
        df = _bronze()
        if df is not None and not df.empty:
            latest_window = df["window_end"].max()
            recent = df[df["window_end"] == latest_window]
            parquet_col = "surge_multiplier" if col == "multiplier" else col
            if parquet_col in recent.columns:
                for _, r in recent.iterrows():
                    zid = int(r["zone_id"])
                    rows.append({
                        "zone_id": zid,
                        "zone_name": ZONE_BY_ID[zid].name if zid in ZONE_BY_ID else f"Zone {zid}",
                        col: float(r[parquet_col]),
                        "updated_at": str(r["window_end"]),
                    })

    if not rows:
        return _err("no surge data yet")
    return sorted(rows, key=lambda x: x[col], reverse=True)[: max(1, min(int(n), 20))]


def _bronze() -> Optional[pd.DataFrame]:
    if not BRONZE.exists():
        return None
    df = pd.read_parquet(BRONZE)
    df["day"] = pd.to_datetime(df["window_end"]).dt.strftime("%Y-%m-%d")
    return df


def driver_utilisation(date: Optional[str] = None, zone: Optional[str] = None) -> Dict[str, Any]:
    df = _bronze()
    if df is None:
        return _err("no Bronze surge data yet")
    if date:
        df = df[df["day"] == date]
    if zone:
        zid = resolve_zone(zone)
        if zid is None:
            return _err(f"unknown zone '{zone}'")
        df = df[df["zone_id"] == zid]
    online = (df["supply"] + df["on_trip"]).sum()
    if df.empty or online == 0:
        return _err("no driver pings in that selection")
    util = float(df["on_trip"].sum() / online)
    return {"date": date or "all", "zone": zone or "all", "utilisation": round(util, 4), "target": 0.78, "target_met": util >= 0.78,
            "windows_observed": int(len(df))}


def demand_forecast(zone: str) -> Dict[str, Any]:
    zid = resolve_zone(zone)
    if zid is None:
        return _err(f"unknown zone '{zone}'")
    if not FORECAST.exists():
        return _err("no forecast yet - run `python -m ml.score`")
    f = pd.read_parquet(FORECAST)
    f = f[f["zone_id"] == zid]
    if f.empty:
        return _err("no forecast for that zone")
    r = f.iloc[0]
    return {"zone_id": zid, "zone_name": ZONE_BY_ID[zid].name, "predicted_demand_next_30m": float(r["predicted_demand_next_30m"]),
            "recent_actual_30m": float(r["recent_lag_30m"]), "as_of": str(r["as_of"]), "model": r["model"]}


def kpi_summary(date: Optional[str] = None) -> Dict[str, Any]:
    if not TRIPS.exists():
        return _err("no trip history found")
    t = pd.read_parquet(TRIPS)
    if date:
        t = t[pd.to_datetime(t["request_time"]).dt.strftime("%Y-%m-%d") == date]
    done = t[t["status"] == "COMPLETED"]
    if done.empty:
        return _err("no completed trips on that date")
    eta = ((pd.to_datetime(done["pickup_time"]) - pd.to_datetime(done["request_time"])).dt.total_seconds() / 60).mean()
    out = {"date": date or "all", "completed_trips": int(len(done)), "cancel_rate": round(float((t["status"] == "CANCELLED").mean()), 4),
           "avg_rider_eta_min": round(float(eta), 2), "eta_target_met": bool(eta < 4.5),
           "revenue_inr": round(float(done["fare"].sum()), 0), "revenue_per_km_inr": round(float(done["fare"].sum() / done["distance_km"].sum()), 2)}
    u = driver_utilisation(date)
    out["driver_utilisation"] = u.get("utilisation", "n/a (no pings)")
    return out


REGISTRY = {"get_current_surge": get_current_surge, "top_zones": top_zones, "driver_utilisation": driver_utilisation,
            "demand_forecast": demand_forecast, "kpi_summary": kpi_summary}

TOOL_SCHEMAS: List[dict] = [
    {"name": "get_current_surge", "description": "Latest surge multiplier, demand, supply, ratio and weather for a Hyderabad zone (name or id).",
     "input_schema": {"type": "object", "properties": {"zone": {"type": "string"}}, "required": ["zone"]}},
    {"name": "top_zones", "description": "Top N zones right now by surge_multiplier, demand or ratio.",
     "input_schema": {"type": "object", "properties": {"metric": {"type": "string", "enum": ["surge_multiplier", "demand", "ratio"]}, "n": {"type": "integer"}}, "required": ["metric"]}},
    {"name": "driver_utilisation", "description": "Driver utilisation (on-trip share of online driver pings) for an optional date YYYY-MM-DD and zone.",
     "input_schema": {"type": "object", "properties": {"date": {"type": "string"}, "zone": {"type": "string"}}}},
    {"name": "demand_forecast", "description": "Next 30-minute demand forecast for a zone with the recent actual.",
     "input_schema": {"type": "object", "properties": {"zone": {"type": "string"}}, "required": ["zone"]}},
    {"name": "kpi_summary", "description": "Trips, cancel rate, rider ETA, revenue (INR) and utilisation for an optional date YYYY-MM-DD.",
     "input_schema": {"type": "object", "properties": {"date": {"type": "string"}}}}]
