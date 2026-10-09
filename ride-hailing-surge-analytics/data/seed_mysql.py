#!/usr/bin/env python3
"""Seed MySQL trips, drivers, riders, and city_zones from TLC HVFHV Parquet.

Driver and rider identities are synthetic (Faker). Full monthly Parquet stays on
disk; only SEED_TRIP_LIMIT rows are inserted so a 16 GB laptop can run MySQL.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pyarrow.parquet as pq
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from data.models import Base, CityZone, Driver, Rider, Trip
from data.settings import TLC_DIR, env_float, env_int, mysql_url, setup_logging
from data.validation import (
    CANCEL_REASONS,
    CANCELLED,
    COMPLETED,
    is_valid_zone_id,
    miles_to_km,
)
from data.zones import load_city_zones

LOGGER = logging.getLogger("seed_mysql")
VEHICLE_TYPES = ("UberX", "UberXL", "Comfort", "Green", "Black")
HOME_CITY = "New York"
HVFHV_COLUMNS = (
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "base_passenger_fare",
)


def _parquet_files() -> list[Path]:
    files = sorted(TLC_DIR.glob("fhvhv_tripdata_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No HVFHV Parquet files in {TLC_DIR}. Run data/download_tlc.py")
    return files


def _iter_tlc_rows(limit: int) -> Iterator[dict[str, Any]]:
    remaining = limit
    for path in _parquet_files():
        LOGGER.info("Reading %s", path.name)
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=min(8192, remaining), columns=list(HVFHV_COLUMNS)):
            frame = batch.to_pandas()
            for rec in frame.to_dict(orient="records"):
                yield rec
                remaining -= 1
                if remaining <= 0:
                    return


def _build_people(
    fake: Faker,
    zone_ids: list[int],
    driver_count: int,
    rider_count: int,
) -> tuple[list[Driver], list[Rider]]:
    drivers: list[Driver] = []
    for _ in range(driver_count):
        drivers.append(
            Driver(
                driver_id=str(uuid.uuid4()),
                name=fake.name(),
                vehicle_type=fake.random_element(VEHICLE_TYPES),
                joined_date=fake.date_between(start_date="-5y", end_date="-30d"),
                home_city=HOME_CITY,
                home_zone_id=fake.random_element(zone_ids),
                rating=Decimal(str(round(fake.pyfloat(min_value=3.5, max_value=5.0), 2))),
            )
        )
    riders: list[Rider] = []
    for _ in range(rider_count):
        riders.append(
            Rider(
                rider_id=str(uuid.uuid4()),
                name=fake.name(),
                joined_date=fake.date_between(start_date="-4y", end_date="-7d"),
                home_city=HOME_CITY,
                rating=Decimal(str(round(fake.pyfloat(min_value=3.0, max_value=5.0), 2))),
            )
        )
    return drivers, riders


def _trip_from_tlc(
    rec: dict[str, Any],
    *,
    rng: random.Random,
    driver_ids: list[str],
    rider_ids: list[str],
    cancel_rate: float,
    invalid_rate: float,
) -> Trip:
    request = rec.get("request_datetime")
    pickup = rec.get("pickup_datetime")
    dropoff = rec.get("dropoff_datetime")
    if request is None:
        request = pickup or datetime.now(tz=UTC).replace(tzinfo=None)
    pickup_zone = _as_int(rec.get("PULocationID"))
    dropoff_zone = _as_int(rec.get("DOLocationID"))
    fare = rec.get("base_passenger_fare")
    fare_dec = None if fare is None or (isinstance(fare, float) and fare != fare) else Decimal(str(round(float(fare), 2)))

    roll = rng.random()
    status = COMPLETED
    cancel_reason = None
    if roll < cancel_rate:
        status = CANCELLED
        cancel_reason = rng.choice(CANCEL_REASONS)
        pickup = None
        dropoff = None
        fare_dec = Decimal("0.00")

    # Intentionally inject a few dirty rows so Phase 2 cleansing has something to drop.
    if roll >= cancel_rate and roll < cancel_rate + invalid_rate:
        kind = rng.choice(("null_zone", "neg_fare", "time_order"))
        if kind == "null_zone":
            pickup_zone = None
        elif kind == "neg_fare":
            fare_dec = Decimal("-1.00")
        elif kind == "time_order" and pickup is not None:
            dropoff = pickup - timedelta(minutes=5)

    now = datetime.now(tz=UTC).replace(tzinfo=None)
    trip_key = f"{request}|{pickup_zone}|{dropoff_zone}|{rng.random()}"
    return Trip(
        trip_id=str(uuid.uuid5(uuid.NAMESPACE_URL, trip_key)),
        driver_id=rng.choice(driver_ids),
        rider_id=rng.choice(rider_ids),
        pickup_zone_id=pickup_zone if is_valid_zone_id(pickup_zone) else pickup_zone,
        dropoff_zone_id=dropoff_zone if is_valid_zone_id(dropoff_zone) else dropoff_zone,
        request_time=_naive(request),
        pickup_time=_naive(pickup),
        dropoff_time=_naive(dropoff),
        distance_km=miles_to_km(rec.get("trip_miles")),
        fare=fare_dec,
        status=status,
        cancel_reason=cancel_reason,
        last_modified=now,
    )


def _as_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _naive(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    return None


def _wait_for_mysql(engine: Engine, attempts: int = 30) -> None:
    last: Exception | None = None
    for _ in range(attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            import time

            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable: {last}") from last


def seed() -> None:
    setup_logging()
    trip_limit = env_int("SEED_TRIP_LIMIT", 50_000)
    driver_count = env_int("SEED_DRIVER_COUNT", 2_000)
    rider_count = env_int("SEED_RIDER_COUNT", 8_000)
    cancel_rate = env_float("SEED_CANCEL_RATE", 0.08)
    invalid_rate = env_float("SEED_INVALID_RATE", 0.01)
    faker_seed = env_int("FAKER_SEED", 42)

    fake = Faker()
    Faker.seed(faker_seed)
    rng = random.Random(faker_seed)

    zones = load_city_zones()
    zone_ids = [z["zone_id"] for z in zones]
    drivers, riders = _build_people(fake, zone_ids, driver_count, rider_count)

    engine = create_engine(mysql_url(), pool_pre_ping=True)
    _wait_for_mysql(engine)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    LOGGER.info("Inserting %s zones, %s drivers, %s riders", len(zones), len(drivers), len(riders))
    with SessionLocal() as session:
        session.add_all([CityZone(**row) for row in zones])
        session.commit()
        _add_in_batches(session, drivers, 500)
        _add_in_batches(session, riders, 500)

        driver_ids = [d.driver_id for d in drivers]
        rider_ids = [r.rider_id for r in riders]
        batch: list[Trip] = []
        inserted = 0
        for rec in _iter_tlc_rows(trip_limit):
            batch.append(
                _trip_from_tlc(
                    rec,
                    rng=rng,
                    driver_ids=driver_ids,
                    rider_ids=rider_ids,
                    cancel_rate=cancel_rate,
                    invalid_rate=invalid_rate,
                )
            )
            if len(batch) >= 1000:
                session.add_all(batch)
                session.commit()
                inserted += len(batch)
                LOGGER.info("Inserted %s / %s trips", inserted, trip_limit)
                batch = []
        if batch:
            session.add_all(batch)
            session.commit()
            inserted += len(batch)

    with engine.connect() as conn:
        counts = {
            table: conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
            for table in ("city_zones", "drivers", "riders", "trips")
        }
        cancelled = conn.execute(text("SELECT COUNT(*) FROM trips WHERE status = 'CANCELLED'")).scalar_one()
        invalidish = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM trips
                WHERE pickup_zone_id IS NULL
                   OR dropoff_zone_id IS NULL
                   OR fare < 0
                   OR (pickup_time IS NOT NULL AND dropoff_time IS NOT NULL AND dropoff_time < pickup_time)
                """
            )
        ).scalar_one()
    LOGGER.info("Seed complete: %s", counts)
    LOGGER.info("Cancelled trips: %s | injected invalid rows: %s", cancelled, invalidish)


def _add_in_batches(session: Session, rows: list[Any], size: int) -> None:
    for i in range(0, len(rows), size):
        session.add_all(rows[i : i + size])
        session.commit()


def main() -> None:
    seed()


if __name__ == "__main__":
    main()
