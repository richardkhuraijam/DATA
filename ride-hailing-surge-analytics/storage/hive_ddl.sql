-- Hive Star Schema DDL
-- Database: ridehail_dw
-- Storage: Parquet partitioned by city and trip_date

CREATE DATABASE IF NOT EXISTS ridehail_dw;
USE ridehail_dw;

-- Dimension 1: Driver
CREATE EXTERNAL TABLE IF NOT EXISTS dim_driver (
    driver_key INT,
    driver_id INT,
    name STRING,
    vehicle_type STRING,
    joined_date DATE,
    home_city STRING,
    rating DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/dim_driver';

-- Dimension 2: Zone
CREATE EXTERNAL TABLE IF NOT EXISTS dim_zone (
    zone_key INT,
    zone_id INT,
    zone_name STRING,
    borough STRING,
    service_zone STRING,
    centroid_lat DOUBLE,
    centroid_lon DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/dim_zone';

-- Dimension 3: Time
CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
    time_key BIGINT,
    full_ts TIMESTAMP,
    date DATE,
    hour INT,
    minute INT,
    day_of_week INT,
    is_weekend INT,
    is_holiday INT
)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/dim_time';

-- Fact Table: Trips
CREATE EXTERNAL TABLE IF NOT EXISTS fact_trips (
    trip_id STRING,
    driver_key INT,
    rider_id INT,
    pickup_zone_key INT,
    dropoff_zone_key INT,
    time_key BIGINT,
    trip_status STRING,
    cancel_reason STRING,
    distance_km DOUBLE,
    duration_min DOUBLE,
    fare_amount DOUBLE,
    surge_multiplier DOUBLE,
    wait_time_min DOUBLE
)
PARTITIONED BY (city STRING, trip_date DATE)
STORED AS PARQUET
LOCATION 'hdfs:///warehouse/fact_trips';
