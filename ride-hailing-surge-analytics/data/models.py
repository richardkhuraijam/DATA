"""SQLAlchemy models for the operational MySQL source."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Index, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CityZone(Base):
    __tablename__ = "city_zones"

    zone_id: Mapped[int] = mapped_column(primary_key=True)
    zone_name: Mapped[str] = mapped_column(String(128), nullable=False)
    borough: Mapped[str] = mapped_column(String(64), nullable=False)
    service_zone: Mapped[str | None] = mapped_column(String(64))
    centroid_lat: Mapped[float | None]
    centroid_lon: Mapped[float | None]


class Driver(Base):
    __tablename__ = "drivers"

    driver_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    vehicle_type: Mapped[str] = mapped_column(String(32), nullable=False)
    joined_date: Mapped[date] = mapped_column(Date, nullable=False)
    home_city: Mapped[str] = mapped_column(String(64), nullable=False)
    home_zone_id: Mapped[int | None] = mapped_column(ForeignKey("city_zones.zone_id"))
    rating: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)


class Rider(Base):
    __tablename__ = "riders"

    rider_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    joined_date: Mapped[date] = mapped_column(Date, nullable=False)
    home_city: Mapped[str] = mapped_column(String(64), nullable=False)
    rating: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)


class Trip(Base):
    __tablename__ = "trips"
    __table_args__ = (
        Index("idx_trips_last_modified", "last_modified"),
        Index("idx_trips_pickup_zone", "pickup_zone_id"),
        Index("idx_trips_status", "status"),
    )

    trip_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    driver_id: Mapped[str] = mapped_column(String(36), ForeignKey("drivers.driver_id"), nullable=False)
    rider_id: Mapped[str] = mapped_column(String(36), ForeignKey("riders.rider_id"), nullable=False)
    # No FK: TLC uses 264/265 as unknown, and Phase 2 cleansing expects dirty zone values.
    pickup_zone_id: Mapped[int | None]
    dropoff_zone_id: Mapped[int | None]
    request_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    pickup_time: Mapped[datetime | None] = mapped_column(DateTime)
    dropoff_time: Mapped[datetime | None] = mapped_column(DateTime)
    distance_km: Mapped[float | None]
    fare: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    cancel_reason: Mapped[str | None] = mapped_column(String(64))
    last_modified: Mapped[datetime] = mapped_column(DateTime, nullable=False)
