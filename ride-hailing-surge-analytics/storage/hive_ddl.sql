-- Hive star schema (Parquet, fact partitioned by city + trip_date). ${hivevar:warehouse} = HDFS/local warehouse dir.
CREATE DATABASE IF NOT EXISTS ridehail_dw;
USE ridehail_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_driver (
  driver_key INT, driver_id STRING, name_hash STRING, vehicle_type STRING, joined_date DATE, home_city STRING, rating DOUBLE)
STORED AS PARQUET LOCATION '${hivevar:warehouse}/dim_driver';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_zone (
  zone_key INT, zone_id INT, zone_name STRING, zone_type STRING, city STRING, centroid_lat DOUBLE, centroid_lon DOUBLE)
STORED AS PARQUET LOCATION '${hivevar:warehouse}/dim_zone';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
  time_key BIGINT, full_ts TIMESTAMP, `date` DATE, `hour` INT, `minute` INT, day_of_week INT, is_weekend BOOLEAN,
  is_holiday BOOLEAN, peak_flag BOOLEAN)
STORED AS PARQUET LOCATION '${hivevar:warehouse}/dim_time';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_trips (
  trip_id STRING, driver_key INT, rider_id STRING, pickup_zone_key INT, dropoff_zone_key INT, time_key BIGINT,
  trip_status STRING, cancel_reason STRING, distance_km DOUBLE, duration_min DOUBLE, fare_inr DOUBLE, wait_time_min DOUBLE)
PARTITIONED BY (city STRING, trip_date DATE)
STORED AS PARQUET LOCATION '${hivevar:warehouse}/fact_trips';
-- then: MSCK REPAIR TABLE ridehail_dw.fact_trips;
