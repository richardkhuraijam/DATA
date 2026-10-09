"""Spark Core batch job for analyzing historical trips using pair RDDs.

Features:
- Reads Parquet trips data (avoids broken sc.textFile CSV pattern)
- Cleans and filters out invalid trips (null zones, negative fares, dropoff < pickup)
- Performs pair-RDD analyses:
  1. Peak hours ranking: reduceByKey on (hour, 1), sortBy count desc
  2. Cancellation reasons ranking per zone/hour: reduceByKey on ((zone, hour, reason), 1), sortBy count desc
"""

from __future__ import annotations

from datetime import datetime
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pyspark.sql import Row, SparkSession
from dotenv import load_dotenv

from data.validation import is_valid_trip

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("spark_core_trips")


def parse_row_to_dict(row: Row) -> Dict[str, Any]:
    """Convert PySpark Row to a Python dict matching standard schema."""
    row_dict = row.asDict()
    return {
        "trip_id": row_dict.get("trip_id"),
        "driver_id": row_dict.get("driver_id"),
        "rider_id": row_dict.get("rider_id"),
        "pickup_zone_id": row_dict.get("pickup_zone_id"),
        "dropoff_zone_id": row_dict.get("dropoff_zone_id"),
        "request_time": row_dict.get("request_time"),
        "pickup_time": row_dict.get("pickup_time"),
        "dropoff_time": row_dict.get("dropoff_time"),
        "distance_km": row_dict.get("distance_km"),
        "fare": row_dict.get("fare"),
        "status": row_dict.get("status"),
        "cancel_reason": row_dict.get("cancel_reason"),
    }


def filter_valid_trips_rdd(trips_rdd: Any) -> Any:
    """Filter invalid trips from an RDD of row dicts."""
    return trips_rdd.filter(is_valid_trip)


def compute_peak_hours_rdd(valid_trips_rdd: Any) -> List[Tuple[int, int]]:
    """Pair-RDD analysis: Peak hours of trip activity.

    Maps to (pickup_hour, 1), reduces by key, and sorts by count descending.
    """
    def extract_hour(trip: Dict[str, Any]) -> int:
        p_time: datetime | None = trip.get("pickup_time")
        if isinstance(p_time, datetime):
            return p_time.hour
        return 0

    return (
        valid_trips_rdd.map(lambda t: (extract_hour(t), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: kv[1], ascending=False)
        .collect()
    )


def compute_cancellation_reasons_rdd(
    valid_trips_rdd: Any
) -> List[Tuple[Tuple[int, int, str], int]]:
    """Pair-RDD analysis: Cancellation reasons per pickup zone and hour.

    Filters status == 'CANCELLED', maps to ((pickup_zone_id, hour, cancel_reason), 1),
    reduces by key, and sorts by count descending.
    """
    def extract_cancelled_tuple(trip: Dict[str, Any]) -> Tuple[int, int, str]:
        p_zone = int(trip.get("pickup_zone_id") or 0)
        req_time: datetime | None = trip.get("request_time") or trip.get("pickup_time")
        hour = req_time.hour if isinstance(req_time, datetime) else 0
        reason = str(trip.get("cancel_reason") or "UNKNOWN")
        return (p_zone, hour, reason)

    return (
        valid_trips_rdd.filter(lambda t: t.get("status") == "CANCELLED")
        .map(lambda t: (extract_cancelled_tuple(t), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: kv[1], ascending=False)
        .collect()
    )


def run_spark_batch_analysis(
    input_parquet_path: str, spark: SparkSession | None = None
) -> Tuple[List[Tuple[int, int]], List[Tuple[Tuple[int, int, str], int]], int, int]:
    """Execute full Spark Core batch analysis.

    Returns: (peak_hours, cancellation_reasons, total_rows, valid_rows)
    """
    own_session = False
    if spark is None:
        spark = (
            SparkSession.builder.appName("SparkCoreTripsBatch")
            .master("local[*]")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
            .getOrCreate()
        )
        own_session = True

    try:
        logger.info("Reading trips Parquet from: %s", input_parquet_path)
        df = spark.read.parquet(input_parquet_path)
        total_count = df.count()

        # Convert DF to RDD of row dicts
        trips_rdd = df.rdd.map(parse_row_to_dict)

        # Filter valid rows
        valid_rdd = filter_valid_trips_rdd(trips_rdd).cache()
        valid_count = valid_rdd.count()
        dropped_count = total_count - valid_count

        logger.info(
            "Trips count summary — Total: %d, Valid: %d, Dropped (invalid): %d",
            total_count,
            valid_count,
            dropped_count,
        )

        peak_hours = compute_peak_hours_rdd(valid_rdd)
        cancellation_reasons = compute_cancellation_reasons_rdd(valid_rdd)

        logger.info("Top 5 peak hours: %s", peak_hours[:5])
        logger.info("Top 5 cancellation reasons: %s", cancellation_reasons[:5])

        return peak_hours, cancellation_reasons, total_count, valid_count
    finally:
        if own_session and spark:
            spark.stop()


def main() -> None:
    raw_landing_trips = os.getenv(
        "TRIPS_PARQUET_PATH", str(ROOT / "data" / "raw" / "mysql" / "trips")
    )
    if not os.path.exists(raw_landing_trips):
        logger.warning(
            "Path %s does not exist. Run spark_jdbc_import first or pass TRIPS_PARQUET_PATH.",
            raw_landing_trips,
        )
        return

    run_spark_batch_analysis(raw_landing_trips)


if __name__ == "__main__":
    main()
