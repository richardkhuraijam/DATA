"""Synthetic Hyderabad driver fleet. Each driver pings every 15-30 s with status AVAILABLE / ON_TRIP.

Drivers are placed in proportion to zone demand and slowly drift toward busier zones, so supply is correlated with
demand (surge appears where demand outruns it). The share of drivers on a trip rises with citywide demand, so
utilisation is an OUTPUT of the simulation, not a hard-coded number.

  python -m ingestion.producers.gps_simulator --drivers 200
  python -m ingestion.producers.gps_simulator --drivers 200 --simulate-hour 19
"""
from __future__ import annotations

import argparse
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from data.hyderabad import IST_OFFSET, ZONE_BY_ID, ZONES, hour_profile
from ingestion.producers.utils import create_producer, send

logger = logging.getLogger("gps_simulator")


def zone_weights(ist: datetime) -> list[float]:
    wk = ist.weekday() >= 5
    return [z.weight * hour_profile(z.kind, ist.hour + ist.minute / 60, wk) for z in ZONES]


def make_ping(driver_id: str, zone_id: int, status: str, rng: random.Random, now_utc: datetime) -> dict:
    z = ZONE_BY_ID[zone_id]
    return {"event_id": uuid.uuid4().hex, "event_time": now_utc.isoformat(), "driver_id": driver_id, "zone_id": zone_id,
            "lat": round(z.lat + rng.uniform(-0.004, 0.004), 6), "lon": round(z.lon + rng.uniform(-0.004, 0.004), 6),
            "status": status, "source": "gps_simulator"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drivers", type=int, default=200)
    ap.add_argument("--simulate-hour", type=int, default=None)
    ap.add_argument("--base-busy", type=float, default=0.45, help="share of drivers on a trip at average demand")
    ap.add_argument("--max-pings", type=int, default=0)
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    ist0 = (datetime.now(timezone.utc) + IST_OFFSET).replace(tzinfo=None)
    w0 = zone_weights(ist0.replace(hour=a.simulate_hour) if a.simulate_hour is not None else ist0)
    ids = [z.zone_id for z in ZONES]
    fleet = [{"id": f"DRV-{i:04d}", "zone": rng.choices(ids, weights=w0)[0], "busy": rng.random() < a.base_busy,
              "next": time.time() + rng.uniform(0, 20)} for i in range(a.drivers)]
    producer, sent = create_producer(), 0
    logger.info("GPS simulator: %d drivers", a.drivers)
    try:
        while True:
            now = time.time(); now_utc = datetime.now(timezone.utc)
            ist = (now_utc + IST_OFFSET).replace(tzinfo=None)
            if a.simulate_hour is not None:
                ist = ist.replace(hour=a.simulate_hour)
            weights = zone_weights(ist)
            intensity = min(1.0, sum(weights) / (len(weights) * 0.55))               # 0..1 citywide demand level
            p_busy = min(0.92, max(0.15, a.base_busy * (0.6 + 0.9 * intensity)))
            for d in fleet:
                if now < d["next"]:
                    continue
                if rng.random() < 0.10:                                              # drift toward busier zones
                    d["zone"] = rng.choices(ids, weights=weights)[0]
                if rng.random() < 0.20:                                              # trip start/finish
                    d["busy"] = rng.random() < p_busy
                send(producer, "driver_gps_pings", d["zone"], make_ping(d["id"], d["zone"], "ON_TRIP" if d["busy"] else "AVAILABLE", rng, now_utc))
                sent += 1; d["next"] = now + rng.uniform(15, 30)
            producer.poll(0)
            if a.max_pings and sent >= a.max_pings:
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10); logger.info("sent %d pings", sent)


if __name__ == "__main__":
    main()
