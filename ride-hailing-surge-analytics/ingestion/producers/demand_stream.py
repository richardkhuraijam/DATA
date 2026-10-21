"""Live trip-request generator for Hyderabad (replaces the NYC TLC replay).

Rates follow data/hyderabad.py (IT-hub evening peaks, residential morning peaks, airport night peaks, weekend dips),
evaluated at the current IST time. Events carry a unique event_id and a UTC event_time (for dedup + windowing).

  python -m ingestion.producers.demand_stream                       # real IST time
  python -m ingestion.producers.demand_stream --simulate-hour 19    # pin evening peak for a demo
  python -m ingestion.producers.demand_stream --hotspot "Hitech City" --hotspot-multiplier 4 --rain-mm 5
"""
from __future__ import annotations

import argparse
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from data.hyderabad import IST_OFFSET, ZONES, demand_rate_per_min, haversine_km
from ingestion.producers.utils import create_producer, send
from data.hyderabad import ZONE_BY_ID

logger = logging.getLogger("demand_stream")


def poisson(rng: random.Random, lam: float) -> int:
    import math
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def make_event(zone_id: int, now_utc: datetime, rng: random.Random) -> dict:
    z = ZONE_BY_ID[zone_id]
    others = [o for o in ZONES if o.zone_id != zone_id]
    dest = rng.choices(others, weights=[o.weight / (1 + haversine_km(z.lat, z.lon, o.lat, o.lon) / 3.5) for o in others])[0]
    dist = max(0.8, haversine_km(z.lat, z.lon, dest.lat, dest.lon) * 1.35 + rng.gauss(0, 0.6))
    return {"event_id": uuid.uuid4().hex, "event_time": (now_utc + timedelta(milliseconds=rng.randint(0, 999))).isoformat(),
            "trip_id": uuid.uuid4().hex[:12], "zone_id": zone_id, "dropoff_zone_id": dest.zone_id,
            "distance_km": round(dist, 2), "fare": round(40 + 13 * dist, 2), "source": "demand_sim"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tick", type=float, default=2.0, help="seconds between emission rounds")
    ap.add_argument("--scale", type=float, default=1.0, help="multiplies all demand")
    ap.add_argument("--simulate-hour", type=int, default=None, help="pin the IST hour (0-23) instead of real time")
    ap.add_argument("--rain-mm", type=float, default=0.0, help="demand uplift as if raining (does not change weather topic)")
    ap.add_argument("--hotspot", default=None, help="zone name to boost, e.g. 'Hitech City'")
    ap.add_argument("--hotspot-multiplier", type=float, default=4.0)
    ap.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = run forever)")
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    hot = next((z.zone_id for z in ZONES if a.hotspot and z.name.lower() == a.hotspot.lower()), None)
    if a.hotspot and hot is None:
        raise SystemExit(f"unknown zone {a.hotspot!r}; see data/hyderabad.py")
    producer, sent = create_producer(), 0
    logger.info("demand stream started (tick=%ss scale=%s hotspot=%s)", a.tick, a.scale, a.hotspot)
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            ist = (now_utc + IST_OFFSET).replace(tzinfo=None)
            if a.simulate_hour is not None:
                ist = ist.replace(hour=a.simulate_hour)
            for z in ZONES:
                lam = demand_rate_per_min(z, ist, a.scale, a.rain_mm) * a.tick / 60
                if z.zone_id == hot:
                    lam *= a.hotspot_multiplier
                for _ in range(poisson(rng, lam)):
                    send(producer, "trip_requests", z.zone_id, make_event(z.zone_id, now_utc, rng)); sent += 1
            producer.poll(0)
            if a.max_events and sent >= a.max_events:
                break
            time.sleep(a.tick)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10); logger.info("sent %d trip requests", sent)


if __name__ == "__main__":
    main()
