# Ride-Hailing Surge Analytics

Near-real-time NYC cab-aggregator pipeline: ingest trip requests, driver GPS, and weather; compute zone-level demand/supply every 60 seconds on a 5-minute window (1-minute slide); publish a recommended surge multiplier per zone. A batch/lakehouse path stores history, feeds dashboards, trains a 30-minute demand forecast, and powers a GenAI ops assistant.

**Reference city:** New York. **Zone:** TLC LocationID (263 zones).  
**Language:** Python (PySpark). Identities are synthetic (Faker).

This repo is built **phase by phase**. Phase 1 is ready to run. Do not start later phases until the checklist below passes.

## Risks (read before you scale up)

| Risk | What we are doing |
| --- | --- |
| **16 GB RAM** cannot run MySQL + Kafka×3 + Hadoop + Hive + HBase + Spark + Airflow together | Compose **profiles**: `ingestion`, `streaming`, `batch`. Phase 1 starts **MySQL only**. |
| **Sqoop is in the Apache Attic** (retired 2021) | Phase 2 will pin Sqoop 1.4.7 and ship a **Spark JDBC** import as the supported backup. |
| **Databricks Free Edition / DLT** | Phase 5 will try DLT (`@dlt.expect_or_drop`) and fall back to plain Delta streaming with the same checks. |
| **Full HVFHV months in MySQL** | Tens of millions of rows. We **download** 1–2 months of Parquet to disk, but **seed a sample** (`SEED_TRIP_LIMIT`, default 50k) into MySQL. Replay later reads Parquet. |
| **Surge cap 3.0× rarely binds** | Base max 2.5× + rain 0.2 + temp 0.1 = 2.8×. The 3.0× cap is a safety guard. We will revisit the table in Phase 4 rather than silently changing it now. |

## Repo layout

```
ride-hailing-surge-analytics/
  docker/                 Compose + MySQL / Hadoop / Spark configs
  data/                   TLC download, zone map, MySQL seed
  ingestion/sqoop/        Phase 2
  ingestion/producers/    Phase 3
  processing/batch/       Phase 2
  processing/streaming/   Phase 4
  storage/                Phase 4–5
  lakehouse/              Phase 5
  airflow/dags/           Phase 6
  ml/                     Phase 7
  genai/                  Phase 7
  dashboards/             Phase 7 (Power BI is manual)
  tests/                  unit tests now; drills in Phase 8
  docs/
```

## Phase 1 — Setup & data

### What this phase gives you

- Docker Compose with profiles and Spark memory caps
- NYC TLC HVFHV Parquet (1–2 months) + taxi zone lookup/shapefile
- MySQL tables `city_zones`, `drivers`, `riders`, `trips` (Faker IDs)
- Trip columns: `trip_id`, `driver_id`, `rider_id`, pickup/dropoff zone, request/pickup/dropoff time, distance, fare, status, cancel_reason, last_modified
- A small set of **invalid** rows (null zone, negative fare, dropoff before pickup) so Phase 2 cleansing is demonstrable
- Cancelled trips (~8%) so Phase 2 cancellation-reason analysis has data (public HVFHV is mostly completed trips)

### Prerequisites

- **Docker Desktop running** (Compose talks to `docker.sock`; if the daemon is stopped, `make up-ingestion` fails)
- Python 3.11+
- ~2 GB disk for two HVFHV months plus zones
- Network to `d37ci6vzurychx.cloudfront.net`

### Commands

```bash
cd "/Users/richardkhuraijam/Ride _hailing_surge _Analytics"
cp .env.example .env          # edit passwords if you want
python3 -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt

make test                     # unit tests, no Docker required
make up-ingestion             # MySQL only
make download                 # TLC Parquet + zone map (slow; skips files that exist)
make seed                     # pandas + SQLAlchemy → MySQL
```

Optional one-liner after `.env` and the venv exist: `make phase1`.

To load a smaller sample (faster seed):

```bash
SEED_TRIP_LIMIT=5000 make seed
```

To load only one month of Parquet, set `TLC_MONTHS=2026-01` in `.env` before `make download`.

### Expected output

**`make test`**

```
.......                                                                  [100%]
```

**`make up-ingestion`**

- Container `ridehail-mysql-1` (name may vary) **healthy**
- Port `3306` published

**`make download`**

- `data/raw/tlc/fhvhv_tripdata_YYYY-MM.parquet` (one or two files, hundreds of MB each)
- `data/raw/zones/taxi_zone_lookup.csv`
- `data/raw/zones/taxi_zones.zip` (extracted under `data/raw/zones/shapefile/`)

**`make seed`** (defaults)

```
Seed complete: {'city_zones': 263, 'drivers': 2000, 'riders': 8000, 'trips': 50000}
Cancelled trips: ~4000 | injected invalid rows: ~500
```

Counts are approximate except **263 zones**, **2000 drivers**, **8000 riders**, and **SEED_TRIP_LIMIT trips**.

### Verification checklist

1. `docker compose --env-file .env -f docker/docker-compose.yml --profile ingestion ps` shows MySQL healthy. Namenode/datanode should **not** be running (Makefile starts `mysql` only).
2. `python -c "from data.settings import mysql_url; from sqlalchemy import create_engine, text; e=create_engine(mysql_url()); print(e.connect().execute(text('show tables')).fetchall())"` lists `city_zones`, `drivers`, `riders`, `trips`.
3. `SELECT COUNT(*) FROM city_zones;` → 263. Spot-check `zone_id`, `zone_name`, `borough`, `service_zone`, centroids (centroids empty only if the shapefile download failed).
4. `SELECT driver_id, name FROM drivers LIMIT 5;` — names look like Faker, not TLC license numbers.
5. `DESCRIBE trips;` includes `trip_id`, `driver_id`, `rider_id`, `pickup_zone_id`, `dropoff_zone_id`, `request_time`, `pickup_time`, `dropoff_time`, `distance_km`, `fare`, `status`, `cancel_reason`, `last_modified`.
6. `SELECT COUNT(*) FROM trips WHERE fare < 0 OR pickup_zone_id IS NULL OR dropoff_time < pickup_time;` → greater than 0 (dirty rows for Phase 2).
7. `.env` is **not** committed (see `.gitignore`).

MySQL CLI:

```bash
docker compose --env-file .env -f docker/docker-compose.yml --profile ingestion exec mysql \
  mysql -uetl -p"$MYSQL_PASSWORD" ridehail
```

### Compose profiles (later phases)

```bash
make up-streaming    # Kafka x3, host ports 19092–19094
make up-batch        # HDFS, Hive, HBase+Thrift :9090, Spark UI :8080, Airflow :8081
make down            # all profiles
```

Spark memory is capped via `SPARK_WORKER_MEMORY`, `SPARK_DRIVER_MEMORY`, `SPARK_MASTER_MEMORY` in `.env` and `docker/spark/spark-defaults.conf`.

## Phase 2 — Batch Ingestion & Analysis

- **Sqoop Script:** `ingestion/sqoop/sqoop_import.sh`
- **Spark JDBC Import Backup:** `python -m ingestion.sqoop.spark_jdbc_import`
- **Spark Core Pair-RDD Batch Analysis:** `python -m processing.batch.spark_core_trips`

## Phase 3 — Streaming Ingestion (Kafka Producers)

- **Kafka Producers Setup & Run:**
  - Replay TLC Trip Requests: `python -m ingestion.producers.tlc_replay`
  - Driver GPS Simulator: `python -m ingestion.producers.gps_simulator`
  - CityBikes Live Supply Proxy: `python -m ingestion.producers.citybikes`
  - Open-Meteo Weather Stream: `python -m ingestion.producers.weather`

## Phase 4 — Surge Engine (Spark Structured Streaming)

- **Rules & Sinks:** `processing/streaming/surge_rules.py`, `processing/streaming/sinks.py`, `storage/hbase_schema.py`
- **Run Streaming Surge Engine:** `python -m processing.streaming.surge_stream`

## Phase 5 — Warehouse & Lakehouse

- **Hive Star Schema:** `storage/hive_ddl.sql`
- **S3 Sync & Glue Crawler:** `python -m lakehouse.s3_sync`
- **Athena Scan Benchmark:** `python -m lakehouse.athena_benchmark`
- **Databricks DLT Pipeline:** `lakehouse/dlt_pipeline.py`

## Phase 6 — Orchestration (Airflow DAGs)

- `airflow/dags/master_batch_dag.py`
- `airflow/dags/hourly_zone_agg_dag.py`
- `airflow/dags/daily_driver_incentive_payout_dag.py`

## Phase 7 — Analytics, Demand Forecast & GenAI Assistant

- **Train 30-Min Demand Model:** `python -m ml.train`
- **Batch Forecast Scorer:** `python -m ml.score`
- **GenAI Ops Assistant (Streamlit):** `streamlit run genai/assistant_app.py`
- **Power BI Build Sheet:** `dashboards/power_bi_spec.md`

## Phase 8 — Testing, Reconciliation & Failure Drills

- **Run Unit Tests:** `pytest -q`
- **Run Broker Kill Drill:** `python -m tests.drills.broker_kill`
- **Run Spark Restart Drill:** `python -m tests.drills.spark_restart`
- **Run Full Reconciliation:** `python -m tests.reconciliation`

