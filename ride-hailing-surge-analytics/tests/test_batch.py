from datetime import datetime
import pytest
from pyspark.sql import SparkSession

from processing.batch.spark_core_trips import (
    compute_cancellation_reasons_rdd,
    compute_peak_hours_rdd,
    filter_valid_trips_rdd,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("TestBatchAnalysis")
        .master("local[1]")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_batch_filtering_and_aggregations(spark: SparkSession) -> None:
    test_trips = [
        # Valid completed trip at hour 8
        {
            "trip_id": "T101",
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "fare": 15.0,
            "request_time": datetime(2026, 1, 1, 8, 0),
            "pickup_time": datetime(2026, 1, 1, 8, 5),
            "dropoff_time": datetime(2026, 1, 1, 8, 20),
            "status": "COMPLETED",
            "cancel_reason": None,
        },
        # Valid completed trip at hour 8
        {
            "trip_id": "T102",
            "pickup_zone_id": 10,
            "dropoff_zone_id": 30,
            "fare": 25.0,
            "request_time": datetime(2026, 1, 1, 8, 10),
            "pickup_time": datetime(2026, 1, 1, 8, 15),
            "dropoff_time": datetime(2026, 1, 1, 8, 40),
            "status": "COMPLETED",
            "cancel_reason": None,
        },
        # Valid cancelled trip at hour 9
        {
            "trip_id": "T103",
            "pickup_zone_id": 10,
            "dropoff_zone_id": 30,
            "fare": 20.0,
            "request_time": datetime(2026, 1, 1, 9, 0),
            "pickup_time": datetime(2026, 1, 1, 9, 5),
            "dropoff_time": datetime(2026, 1, 1, 9, 10),
            "status": "CANCELLED",
            "cancel_reason": "DRIVER_TAKING_TOO_LONG",
        },
        # Invalid trip: negative fare
        {
            "trip_id": "T104",
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "fare": -5.0,
            "request_time": datetime(2026, 1, 1, 8, 0),
            "pickup_time": datetime(2026, 1, 1, 8, 5),
            "dropoff_time": datetime(2026, 1, 1, 8, 20),
            "status": "COMPLETED",
            "cancel_reason": None,
        },
        # Invalid trip: dropoff before pickup
        {
            "trip_id": "T105",
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "fare": 15.0,
            "request_time": datetime(2026, 1, 1, 8, 0),
            "pickup_time": datetime(2026, 1, 1, 8, 25),
            "dropoff_time": datetime(2026, 1, 1, 8, 20),
            "status": "COMPLETED",
            "cancel_reason": None,
        },
    ]

    rdd = spark.sparkContext.parallelize(test_trips)
    valid_rdd = filter_valid_trips_rdd(rdd).cache()

    # Out of 5 trips, 3 should be valid
    assert valid_rdd.count() == 3

    # Peak hours check: hour 8 should have 2 trips, hour 9 should have 1 trip
    peak_hours = compute_peak_hours_rdd(valid_rdd)
    assert peak_hours == [(8, 2), (9, 1)]

    # Cancellation reasons check
    cancellations = compute_cancellation_reasons_rdd(valid_rdd)
    assert len(cancellations) == 1
    assert cancellations[0] == ((10, 9, "DRIVER_TAKING_TOO_LONG"), 1)
