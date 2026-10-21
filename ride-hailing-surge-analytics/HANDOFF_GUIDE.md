# Hyderabad Ride-Hailing Surge Analytics — Handoff Guide

This repository contains a complete, verified end-to-end data platform for ride-hailing surge analytics in Hyderabad.

---

## 📌 Project Architecture & Current Status

```
[Kafka Cluster x3] ──► [Spark Structured Streaming] ──► [HBase Thrift :9090]
         ▲                       │                               ▲
         │ (Telemetries)         ├──► [Bronze Parquet]           │
[Producers: Weather, GPS, Demand]└──► [Databricks DLT] ───► [SQL / Power BI]
                                                                 │
[FastAPI Dashboard :8000] ◄──────────────────────────────────────┤
[Streamlit GenAI Ops Assistant :8501 (Google Gemini)] ◄──────────┘
```

### ✅ What Has Been Completed & Verified
1. **Tests**: All 40 unit and integration tests passing (`make test`).
2. **Batch Storage**:
   - Cleaned trips imported to `data/raw/mysql/trips`.
   - Hive dimensional star schema built (`dim_driver`, `dim_zone`, `dim_time`, `fact_trips` with 396,633 rows partitioned by date).
3. **Machine Learning Demand Forecast**:
   - Random Forest model trained on 30-min bins: **$R^2 = 0.7595$, $\text{MAE} = 2.20$, $\text{RMSE} = 3.00$** (Passes SLA $\ge 0.70$).
   - Current 30-min predictions scored for all 40 Hyderabad zones in `data/lakehouse/gold_demand_forecast/forecast_latest.parquet`.
4. **Real-Time Streaming Engine**:
   - 3-broker KRaft Kafka cluster (`localhost:19092, 19093, 19094`).
   - HBase table `surge_multipliers` running on port `9090`.
   - Producers active: Open-Meteo weather poller, 200 GPS drivers, and live peak-hour demand generator.
   - Spark Structured Streaming running with 5-min sliding window and 1-min triggers.
5. **Interactive Frontends**:
   - **Operations & Surge Dashboard**: Live at **http://127.0.0.1:8000**
   - **GenAI Ops Assistant (Gemini)**: Live at **http://127.0.0.1:8501**
6. **Cloud Lakehouse**:
   - Databricks Delta Live Tables (DLT) running on Databricks Serverless (`/Volumes/workspace/default/landing`).

---

## 🚀 Quick Setup for Your Friend

### 1. Environment Requirements
- **macOS** or **Linux**
- **Docker Desktop** (running, 8–12 GB RAM recommended; enable Rosetta on Apple Silicon)
- **Python 3.11** (`brew install python@3.11`)
- **Java 17** (`brew install openjdk@17 && export JAVA_HOME=$(/usr/libexec/java_home -v 17)`)

### 2. Activate Virtual Environment
```bash
cd hyd
source .venv/bin/activate
# or if creating a new venv:
# python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

### 3. Verify System Health
```bash
make test         # Runs full pytest suite (all 40 should pass)
make demo-status  # Checks status of streaming containers and demo processes
```

---

## 🎯 The Last Two Steps: Instructions & How to Edit

---

### STEP 9: Airflow Batch Orchestration

Airflow automates daily batch processing, star schema rebuilding, and driver incentive payouts.

#### 1. What Files to Edit
The DAG definitions are located in `airflow/dags/`:
- `airflow/dags/master_batch_dag.py`: Master workflow coordinating daily data ingestion, Spark Core analysis, and Hive star schema.
- `airflow/dags/daily_driver_incentive_payout_dag.py`: Calculates daily driver hours, surge bonuses, and driver payout tables.
- `airflow/dags/hourly_zone_agg_dag.py`: Aggregates hourly demand and ride volume by zone.

#### 2. How to Start Airflow
Run from the project root:
```bash
make up-batch
```
This spins up PostgreSQL (`airflow-db`), MySQL, and the custom Airflow container.

#### 3. Accessing the Airflow UI
- Open browser: **[http://localhost:8081](http://localhost:8081)**
- **Username**: `admin`
- **Password**: Retrieve the auto-generated password by running:
  ```bash
  docker logs ridehail-airflow-1 2>&1 | grep -i password
  ```

#### 4. Triggering the DAG
1. Toggle the pause switch to **Active (On)** for `master_batch_dag`.
2. Click the **Trigger DAG (▶)** button on the right.
3. Click into **Graph View** to watch the tasks run in sequence:
   `check_mysql` ➔ `spark_jdbc_import` ➔ `spark_core_analytics` ➔ `build_star_schema` ➔ `calculate_payouts`.

#### 5. Stopping Airflow
When done, you can stop the batch containers:
```bash
docker compose --env-file .env -f docker/docker-compose.yml --profile batch down
```

---

### STEP 10: Power BI Dashboard & Executive Reporting

Power BI visualizes both historical batch KPIs and real-time streaming surge data.

#### 1. Specification File
Open the complete build sheet:
`dashboards/power_bi_spec.md`

This document details all tables, relationships, DAX measures, and visual formatting.

#### 2. Connecting to Databricks Lakehouse (Gold Tables)
1. Open **Power BI Desktop**.
2. Click **Get Data** ➔ search for **Databricks** ➔ **Connect**.
3. Enter connection parameters from your Databricks SQL Warehouse:
   - **Server Hostname**: `dbc-43ae0b50-9bd0.cloud.databricks.com`
   - **HTTP Path**: Found in Databricks under *SQL Warehouses* ➔ *Serverless Starter Warehouse* ➔ *Connection Details*.
4. Select **DirectQuery** mode (recommended for real-time live map refresh).
5. Load the target tables:
   - `workspace.default.zone_hourly_kpis`
   - `workspace.default.driver_daily_utilisation`
   - `workspace.default.revenue_per_km`
   - `workspace.default.zone_minute_surge`

#### 3. Key DAX Measures to Add
Copy and paste from `dashboards/power_bi_spec.md`:
```dax
// 1. Average Rider Wait Time / ETA
Avg_Rider_ETA = AVERAGE(fact_trips[wait_time_min])

// 2. Driver Utilisation Percentage
Driver_Utilisation_Rate = 
DIVIDE(
    SUM(fact_trips[duration_min]),
    SUM(dim_driver[online_minutes]),
    0
)

// 3. Revenue Per KM
Revenue_Per_KM = 
DIVIDE(
    SUM(fact_trips[fare_amount]),
    SUM(fact_trips[distance_km]),
    0
)
```

#### 4. Recommended Visual Layout
- **Page 1: Real-Time Surge & Supply Map**
  - Bubble Map: Latitude / Longitude from `dim_zone`, bubble size by `demand`, color by `surge_multiplier`.
  - Cards: Current Average Surge, Active Drivers, Total Requests.
- **Page 2: Executive Financials & Efficiency**
  - Line Chart: Hourly revenue vs request volume.
  - Bar Chart: Top 10 zones by demand / revenue.
  - KPI Gauges: Driver utilisation rate (Target: 78%) and Average ETA (Target: < 4.5 min).

---

## 🛠️ Handy Management Commands

| Command | Action |
|---|---|
| `make demo` | Start all 4 streaming processes (weather, demand, gps, engine) |
| `make demo-status` | Check PIDs and Docker container status |
| `make demo-stop` | Cleanly stop all demo streaming processes |
| `make dashboard` | Start local Operations Web Dashboard on http://127.0.0.1:8000 |
| `make assistant` | Start Streamlit GenAI Assistant on http://127.0.0.1:8501 |
| `make analyze` | Re-run Spark Core pair-RDD trip analysis |
| `make star` | Re-build Hive Star Schema in `data/warehouse/` |
| `make train` | Re-train ML Demand Forecast model |
| `make score` | Re-score upcoming 30-min zone demand |
| `make down` | Stop all Docker containers |
