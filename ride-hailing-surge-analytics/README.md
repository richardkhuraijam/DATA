# Ride-Hailing Surge Analytics - Hyderabad

Near-real-time surge pricing for Hyderabad: Kafka -> Spark Structured Streaming (5-min window, 1-min slide, 60 s trigger)
-> HBase / Kafka / Bronze, plus Hive star schema, S3 + Glue + Athena, Databricks DLT, Airflow, demand forecast, GenAI ops assistant.
Python / PySpark. See `docs/FIXES.md` for what was wrong in the earlier version.

> **Data honesty:** there is no public Hyderabad ride-hailing dataset. Trips and live demand are **synthetic** with realistic
> structure (IT-corridor evening peaks, morning residential peaks, airport night peaks, weekend dips, monsoon rain uplift).
> Weather is real (Open-Meteo). Say this in your report; every KPI below is a property of this simulation, not of real riders.

## What has actually been run

| Verified (tests pass: 33) | NOT verified yet - you must run these |
|---|---|
| Surge rules; real Structured Streaming query on a local file source (dedup, distinct drivers, weather join, smoothing, sinks) | Docker stack (Kafka x3, HBase, MySQL, Airflow) |
| Seeder (on SQLite), Spark Core pair-RDD job, star-schema build (Parquet), forecast train/score, Gold jobs, GenAI tool layer | Kafka source/sink, HBase writes, Delta mode, Hive registration |
| Reconciliation logic, drill verdict logic | S3 / Glue / Athena, Databricks DLT, Airflow runs, Streamlit + LLM, both failure drills |

Measured on the synthetic data: forecast R2 = 0.757 (400k trips; 0.28 with 50k - too few events per zone-half-hour),
average request-to-pickup time 5.3 min (**misses the 4.5 min target**; this is a generator parameter, see below).
Not measured yet: surge refresh gap, driver utilisation, Athena scan cut, both drills.

## Run it on a Mac (step by step)

0. **Tools**: Docker Desktop (Settings -> General: enable *Rosetta for x86_64/amd64 emulation* on Apple Silicon; Resources: 10-12 GB RAM),
   `brew install openjdk@17 python@3.11`, then `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
1. **Env**: `python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt && cp .env.example .env`
   (edit passwords). Run `make test` - expect all green.
2. **MySQL + data**: `make up-ingestion`, then `SEED_TRIP_LIMIT=50000 make seed` first (fast), later 400000 for the forecast.
3. **Batch**: `make import` (MySQL -> `data/raw/mysql/*` Parquet; downloads the MySQL JDBC jar once), `make analyze`, `make star`.
4. **Forecast**: `make train` (R2 / MAE / RMSE printed honestly), `make score`.
5. **Streaming stack**: `make up-streaming`, wait ~1 min (HBase is slow under emulation), `make topics`, `make hbase-table`.
6. **Run (4 terminals, each `source .venv/bin/activate`)**: `make weather`, `make gps`, `make demand`, then `make engine`.
   Demo: `python -m ingestion.producers.demand_stream --simulate-hour 19 --hotspot "Hitech City" --rain-mm 5`.
   The engine logs one line per minute, e.g. `batch 3: 40 zones, top surge 2.40x in zone 1`. Verify:
   `python -c "from genai.tools import top_zones; print(top_zones('surge_multiplier',5))"` (needs `SURGE_STORE=hbase`).
7. **Reconciliation (Drill 3)**: also run `make archive` while producers run, then
   `python -m tests.reconciliation --from <UTC start> --to <UTC end>`.
8. **Drills**: `python -m tests.drills.broker_kill` and `python -m tests.drills.spark_restart` (container name via `--container`; check `docker ps`).
9. **Cloud**: `aws configure`, set `S3_BUCKET`, `ATHENA_OUTPUT`, create the Glue crawler over `curated/` and `raw_csv/`, then
   `python -m lakehouse.s3_sync --crawler` and `python -m lakehouse.athena_benchmark --date YYYY-MM-DD`.
10. **Airflow**: `make up-batch`, http://localhost:8081 (password: `docker logs ridehail-airflow-1 | grep -i password`).
11. **GenAI**: set `ANTHROPIC_API_KEY`, `streamlit run genai/assistant_app.py`. **Power BI**: `dashboards/power_bi_spec.md`.

Local test mode without Kafka/HBase: `SURGE_SOURCE=files SURGE_STORE=memory` (see `tests/test_surge_engine.py`).

## Design notes / deviations from the proposal (disclose these)
- Supply = distinct AVAILABLE drivers (GPS simulator); CityBikes is dropped (not available for Hyderabad). Driver utilisation is an *output* of the simulator.
- Zones are locality centroids (approximate), not GHMC polygons; GPS->zone uses nearest centroid in the producer.
- Hive tables are registered through Spark's metastore (`--register-hive`); no HiveServer2 container.
- HDFS is optional (`hdfs` profile); code defaults to local paths. Sqoop is replaced by the Spark JDBC import (Sqoop is retired).
- HBase "latest for a zone" uses a bounded forward range scan instead of a reverse scan.
- Weather join is inside `foreachBatch`. Temperature rule (>35 C -> +0.1x) fires often in a Hyderabad summer; thresholds are in `surge_rules.py`.
- A zone with no events for a whole minute is written as demand 0 / supply 0 (no surge).
- Payout utilisation uses an explicit assumption (`ONLINE_HOURS_PER_DAY`) because historical trips have no online-time source.
- Average ETA on history is a generator parameter (wait times); the live surge->supply feedback loop is not simulated.

## Layout
`data/` Hyderabad model + seeder | `ingestion/producers/` demand, GPS, weather, topics | `processing/streaming/` engine, sinks, archive |
`processing/batch/` Spark Core | `processing/gold/` hourly KPIs, payouts | `storage/` HBase, Hive DDL, star schema | `lakehouse/` S3, Athena, DLT |
`airflow/dags/` | `ml/` | `genai/` | `tests/` (unit, engine, reconciliation, drills) | `docker/`
