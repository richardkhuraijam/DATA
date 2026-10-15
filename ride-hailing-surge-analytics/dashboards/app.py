"""Hyderabad Ride-Hailing Surge Analytics - Local Operations & Monitoring Dashboard.

SECURITY NOTE:
CRITICAL SECURITY: This server MUST bind to 127.0.0.1 ONLY.
It launches and terminates local operating system processes (producers, Spark streaming engine)
and reads local log files. Binding to 0.0.0.0 or any public network interface would expose
unauthenticated remote process execution and system controls. Never change host from 127.0.0.1.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import fastapi
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import joblib
import pandas as pd
import uvicorn

from data.hyderabad import ZONES, ZONE_BY_ID
from dashboards import runner
from storage.hbase_schema import get_store

ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("dashboard_app")

app = FastAPI(title="Hyderabad Surge Analytics Dashboard", docs_url=None, redoc_url=None)


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Index HTML not found")
    return HTMLResponse(content=index_file.read_text(encoding="utf-8"))


@app.get("/api/status")
async def get_status():
    """Return status of all managed demo processes and Docker containers."""
    demo_status = runner.get_demo_status()
    docker_status = runner.get_docker_status()
    return JSONResponse(content={"processes": demo_status, "docker": docker_status})


@app.post("/api/demo/start")
async def start_demo_pipeline():
    """Start all demo processes: weather, demand, gps, engine."""
    results = runner.start_all()
    return JSONResponse(content={"action": "start_all", "results": results})


@app.post("/api/demo/stop")
async def stop_demo_pipeline():
    """Stop all demo processes cleanly."""
    results = runner.stop_all()
    return JSONResponse(content={"action": "stop_all", "results": results})


@app.post("/api/process/{name}/start")
async def start_single_process(name: str):
    res = runner.start_process(name)
    return JSONResponse(content=res)


@app.post("/api/process/{name}/stop")
async def stop_single_process(name: str):
    res = runner.stop_process(name)
    return JSONResponse(content=res)


@app.get("/api/surge")
async def get_live_surge():
    """Return live surge pricing for all 40 Hyderabad zones from HBase.

    Handles 'no data yet' gracefully if the engine has not written its first micro-batch.
    """
    try:
        store = get_store()
        zone_data = []
        has_any_data = False

        for z in ZONES:
            record = store.get_latest_surge(z.zone_id)
            if record:
                has_any_data = True
                zone_data.append({
                    "zone_id": z.zone_id,
                    "name": z.name,
                    "kind": z.kind,
                    "lat": z.lat,
                    "lon": z.lon,
                    "surge_multiplier": float(record.get("multiplier", 1.0)),
                    "demand": int(record.get("demand", 0)),
                    "supply": int(record.get("supply", 0)),
                    "on_trip": int(record.get("on_trip", 0)),
                    "ratio": float(record.get("ratio", 1.0)),
                    "updated_at": str(record.get("updated_at", "")),
                })
            else:
                zone_data.append({
                    "zone_id": z.zone_id,
                    "name": z.name,
                    "kind": z.kind,
                    "lat": z.lat,
                    "lon": z.lon,
                    "surge_multiplier": 1.0,
                    "demand": 0,
                    "supply": 0,
                    "on_trip": 0,
                    "ratio": 1.0,
                    "updated_at": None,
                })

        # Sort zones by surge multiplier descending
        top_surge = sorted(zone_data, key=lambda x: x["surge_multiplier"], reverse=True)

        return JSONResponse(content={
            "has_data": has_any_data,
            "count": len(zone_data),
            "zones": top_surge,
        })
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error fetching surge data from HBase: %s", exc)
        return JSONResponse(content={
            "has_data": False,
            "error": str(exc),
            "zones": [
                {
                    "zone_id": z.zone_id,
                    "name": z.name,
                    "kind": z.kind,
                    "lat": z.lat,
                    "lon": z.lon,
                    "surge_multiplier": 1.0,
                    "demand": 0,
                    "supply": 0,
                    "on_trip": 0,
                    "ratio": 1.0,
                    "updated_at": None,
                }
                for z in ZONES
            ],
        })


@app.get("/api/logs/{name}")
async def get_logs(name: str, lines: int = Query(100, ge=10, le=500)):
    """Fetch the last N lines of a specific process log."""
    tail = runner.get_log_tail(name, lines=lines)
    return JSONResponse(content={"name": name, "lines": tail})


@app.get("/api/batch-stats")
async def get_batch_stats():
    """Return historical batch analysis and ML demand forecast performance metrics."""
    stats: Dict[str, Any] = {
        "model": None,
        "warehouse": {},
        "raw_trips": None,
    }

    # Model metrics
    model_file = ROOT / "ml" / "model.joblib"
    if model_file.exists():
        try:
            m = joblib.load(model_file)
            stats["model"] = {
                "name": m.get("name", "random_forest"),
                "metrics": m.get("metrics", {}),
                "features": m.get("features", []),
                "sla_passed": m.get("metrics", {}).get("random_forest", {}).get("r2", 0) >= 0.70,
            }
        except Exception as exc:
            stats["model_error"] = str(exc)

    # Star schema counts
    wh_dir = ROOT / "data" / "warehouse"
    if wh_dir.exists():
        for table in ["dim_driver", "dim_zone", "dim_time", "fact_trips"]:
            t_path = wh_dir / table
            if t_path.exists():
                try:
                    df = pd.read_parquet(t_path)
                    stats["warehouse"][table] = len(df)
                except Exception:
                    pass

    # Raw trips count
    trips_path = ROOT / "data" / "raw" / "mysql" / "trips"
    if trips_path.exists():
        try:
            df = pd.read_parquet(trips_path)
            stats["raw_trips"] = {
                "total": len(df),
                "completed": int((df["status"] == "COMPLETED").sum()),
                "cancelled": int((df["status"] == "CANCELLED").sum()),
            }
        except Exception:
            pass

    return JSONResponse(content=stats)


def main():
    # Bind to 127.0.0.1 ONLY for safety
    host = "127.0.0.1"
    port = int(os.getenv("DASHBOARD_PORT", "8000"))
    logger.info("Starting Hyderabad Surge Dashboard on http://%s:%d (localhost only)", host, port)
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
