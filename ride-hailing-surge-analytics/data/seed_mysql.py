#!/usr/bin/env python3
"""Seed MySQL (city_zones, drivers, riders, trips) with SYNTHETIC Hyderabad data.

No public Hyderabad ride-hailing dataset exists, so trips come from data/hyderabad.py (realistic hourly/zone/rain
structure). Identities are Faker-generated. A small share of cancelled and deliberately invalid rows is injected so
the Phase-2 cleansing job has something real to drop. Also writes data/synthetic/weather_hourly.parquet for ML.
"""
from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from data.hyderabad import CANCEL_REASONS, CITY, VEHICLE_TYPES, ZONES, generate_history, zones_as_rows
from data.models import Base, CityZone, Driver, Rider, Trip
from data.settings import SYNTH_DIR, env_float, env_int, mysql_url, setup_logging

LOGGER = logging.getLogger("seed_mysql")
COMPLETED, CANCELLED = "COMPLETED", "CANCELLED"


def _people(fake: Faker, n_drivers: int, n_riders: int) -> tuple[list[Driver], list[Rider]]:
    zone_ids = [z.zone_id for z in ZONES]
    drivers = [Driver(driver_id=str(uuid.uuid4()), name=fake.name(), vehicle_type=fake.random_element(VEHICLE_TYPES),
                      joined_date=fake.date_between(start_date="-5y", end_date="-30d"), home_city=CITY,
                      home_zone_id=fake.random_element(zone_ids),
                      rating=Decimal(str(round(fake.pyfloat(min_value=3.5, max_value=5.0), 2)))) for _ in range(n_drivers)]
    riders = [Rider(rider_id=str(uuid.uuid4()), name=fake.name(), joined_date=fake.date_between(start_date="-4y", end_date="-7d"),
                    home_city=CITY, rating=Decimal(str(round(fake.pyfloat(min_value=3.0, max_value=5.0), 2)))) for _ in range(n_riders)]
    return drivers, riders


def _to_trip(rec: dict[str, Any], rng: random.Random, driver_ids: list[str], rider_ids: list[str],
             cancel_rate: float, invalid_rate: float, now: datetime) -> Trip:
    pickup, dropoff, fare = rec["pickup_time"], rec["dropoff_time"], Decimal(str(rec["fare"]))
    pz, dz = rec["pickup_zone_id"], rec["dropoff_zone_id"]
    status, reason = COMPLETED, None
    roll = rng.random()
    if roll < cancel_rate:
        status, reason, pickup, dropoff, fare = CANCELLED, rng.choice(CANCEL_REASONS), None, None, Decimal("0.00")
    elif roll < cancel_rate + invalid_rate:                       # dirty rows for the cleansing job
        kind = rng.choice(("null_zone", "neg_fare", "time_order"))
        if kind == "null_zone":
            pz = None
        elif kind == "neg_fare":
            fare = Decimal("-1.00")
        else:
            dropoff = pickup - timedelta(minutes=5)
    return Trip(trip_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)), driver_id=rng.choice(driver_ids),
                rider_id=rng.choice(rider_ids), pickup_zone_id=pz, dropoff_zone_id=dz, request_time=rec["request_time"],
                pickup_time=pickup, dropoff_time=dropoff, distance_km=rec["distance_km"], fare=fare, status=status,
                cancel_reason=reason, last_modified=now)


def _wait_for_mysql(engine: Engine, attempts: int = 30) -> None:
    import time
    last: Exception | None = None
    for _ in range(attempts):
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable: {last}") from last


def seed(engine: Engine | None = None) -> dict[str, int]:
    setup_logging()
    n_trips = env_int("SEED_TRIP_LIMIT", 50_000)
    days = env_int("SEED_DAYS", 30)
    seed_val = env_int("FAKER_SEED", 42)
    fake = Faker("en_IN"); Faker.seed(seed_val)
    rng = random.Random(seed_val)
    cancel_rate, invalid_rate = env_float("SEED_CANCEL_RATE", 0.08), env_float("SEED_INVALID_RATE", 0.01)

    LOGGER.info("Generating ~%s synthetic Hyderabad trips over %s days", n_trips, days)
    trips, weather = generate_history(n_trips, days=days, seed=seed_val)
    SYNTH_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(weather).to_parquet(SYNTH_DIR / "weather_hourly.parquet", index=False)

    drivers, riders = _people(fake, env_int("SEED_DRIVER_COUNT", 2_000), env_int("SEED_RIDER_COUNT", 8_000))
    engine = engine or create_engine(mysql_url(), pool_pre_ping=True)
    _wait_for_mysql(engine)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session_ = sessionmaker(bind=engine)
    now = datetime.utcnow()
    with Session_() as s:
        s.add_all([CityZone(**z) for z in zones_as_rows()]); s.commit()
        for rows in (drivers, riders):
            _batches(s, rows, 500)
        d_ids, r_ids = [d.driver_id for d in drivers], [r.rider_id for r in riders]
        _batches(s, [_to_trip(t, rng, d_ids, r_ids, cancel_rate, invalid_rate, now) for t in trips], 1000)
    with engine.connect() as c:
        counts = {t: c.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one() for t in ("city_zones", "drivers", "riders", "trips")}
        cancelled = c.execute(text("SELECT COUNT(*) FROM trips WHERE status = 'CANCELLED'")).scalar_one()
        dirty = c.execute(text("SELECT COUNT(*) FROM trips WHERE pickup_zone_id IS NULL OR fare < 0 OR "
                               "(pickup_time IS NOT NULL AND dropoff_time < pickup_time)")).scalar_one()
    LOGGER.info("Seed complete: %s | cancelled=%s | injected invalid=%s", counts, cancelled, dirty)
    return counts


def _batches(session: Session, rows: list[Any], size: int) -> None:
    for i in range(0, len(rows), size):
        session.add_all(rows[i:i + size]); session.commit()


if __name__ == "__main__":
    seed()
