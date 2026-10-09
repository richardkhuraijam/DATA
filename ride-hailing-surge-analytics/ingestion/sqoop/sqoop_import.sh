#!/usr/bin/env bash
# Sqoop 1.4.7 incremental and full import script for MySQL -> HDFS raw landing zone
# Triggered by Airflow BashOperator or executed manually.

set -euo pipefail

MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DATABASE="${MYSQL_DATABASE:-ridehail}"
MYSQL_USER="${MYSQL_USER:-etl}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-change_me_etl}"
HDFS_RAW_DIR="${HDFS_RAW_DIR:-/raw/mysql}"

echo "Starting Sqoop ingestion from MySQL (${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}) into HDFS (${HDFS_RAW_DIR})..."

# 1. Incremental import of trips table
echo "Importing table: trips (incremental lastmodified)..."
sqoop import \
  --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}" \
  --username "${MYSQL_USER}" \
  --password "${MYSQL_PASSWORD}" \
  --table trips \
  --target-dir "${HDFS_RAW_DIR}/trips" \
  --as-parquetfile \
  --incremental lastmodified \
  --check-column last_modified \
  --merge-key trip_id \
  -m 4

# 2. Full import of dimension tables
for table in drivers riders city_zones; do
  echo "Importing table: ${table} (full)..."
  sqoop import \
    --connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}" \
    --username "${MYSQL_USER}" \
    --password "${MYSQL_PASSWORD}" \
    --table "${table}" \
    --target-dir "${HDFS_RAW_DIR}/${table}" \
    --as-parquetfile \
    --delete-target-dir \
    -m 1
done

echo "Sqoop ingestion complete."
