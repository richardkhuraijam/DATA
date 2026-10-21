COMPOSE := docker compose --env-file .env -f docker/docker-compose.yml
PYTHON  ?= $(if $(wildcard $(PWD)/.venv/bin/python),$(PWD)/.venv/bin/python,python3)
export PYTHONPATH := $(PWD)
export PYSPARK_PYTHON := $(if $(wildcard $(PWD)/.venv/bin/python),$(PWD)/.venv/bin/python,python3)
export PYSPARK_DRIVER_PYTHON := $(if $(wildcard $(PWD)/.venv/bin/python),$(PWD)/.venv/bin/python,python3)

.PHONY: help env-check up-ingestion up-streaming up-batch down ps seed topics hbase-table test import analyze star engine demand gps weather archive train score demo demo-stop demo-status dashboard

help:
	@echo "make up-ingestion  MySQL            | make seed       synthetic Hyderabad data -> MySQL"
	@echo "make up-streaming  Kafka x3 + HBase | make topics     create Kafka topics     | make hbase-table"
	@echo "make demand|gps|weather  producers   | make engine     surge engine (Spark Structured Streaming)"
	@echo "make import analyze star             | MySQL->parquet, Spark Core job, Hive star schema"
	@echo "make train score                     | demand forecast  | make test | make down"

env-check:
	@test -f .env || (echo "Copy .env.example to .env first" && exit 1)
up-ingestion: env-check
	$(COMPOSE) --profile ingestion up -d mysql
up-streaming: env-check
	$(COMPOSE) --profile streaming up -d
up-batch: env-check
	$(COMPOSE) --profile batch up -d --build
down:
	$(COMPOSE) --profile ingestion --profile streaming --profile batch --profile hdfs down
ps:
	$(COMPOSE) ps -a
seed: env-check
	$(PYTHON) -m data.seed_mysql
topics: env-check
	$(PYTHON) -m ingestion.producers.utils
hbase-table: env-check
	$(PYTHON) -m storage.hbase_schema
demand:
	$(PYTHON) -m ingestion.producers.demand_stream
gps:
	$(PYTHON) -m ingestion.producers.gps_simulator
weather:
	$(PYTHON) -m ingestion.producers.weather
engine: env-check
	$(PYTHON) -m processing.streaming.surge_stream
archive: env-check
	$(PYTHON) -m processing.streaming.archive_raw_stream
import: env-check
	$(PYTHON) -m ingestion.sqoop.spark_jdbc_import
analyze:
	$(PYTHON) -m processing.batch.spark_core_trips
star:
	$(PYTHON) -m storage.build_star_schema
train:
	$(PYTHON) -m ml.train
score:
	$(PYTHON) -m ml.score
demo: env-check
	$(PYTHON) -m dashboards.runner start
demo-stop: env-check
	$(PYTHON) -m dashboards.runner stop
demo-status: env-check
	$(PYTHON) -m dashboards.runner status
dashboard: env-check
	@echo "Starting Hyderabad Surge Dashboard on http://127.0.0.1:8000 (binds ONLY to localhost for safety)..."
	$(PYTHON) -m dashboards.app
assistant: env-check
	@echo "Starting Hyderabad Surge GenAI Ops Assistant on http://127.0.0.1:8501..."
	$(PYTHON) -m streamlit run genai/assistant_app.py --server.port 8501 --server.address 127.0.0.1
test:
	$(PYTHON) -m pytest -q
