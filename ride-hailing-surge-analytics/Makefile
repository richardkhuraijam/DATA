COMPOSE := docker compose --env-file .env -f docker/docker-compose.yml
PYTHON  ?= python3
export PYTHONPATH := $(PWD)

.PHONY: help env-check up-ingestion up-streaming up-batch down ps logs-mysql download seed test mysql-cli phase1

help:
	@echo "Ride-Hailing Surge Analytics"
	@echo "  make up-ingestion   MySQL only (Phase 1)"
	@echo "  make download       NYC TLC HVFHV Parquet + taxi zone map"
	@echo "  make seed           Load zones/drivers/riders/trips into MySQL"
	@echo "  make test           pytest"
	@echo "  make up-streaming   Kafka x3 (Phase 3)"
	@echo "  make up-batch       HDFS, Hive, HBase, Spark, Airflow (Phase 2+)"
	@echo "  make down           Stop all profiles"
	@echo "  make mysql-cli      mysql client inside the container"

env-check:
	@test -f .env || (echo "Copy .env.example to .env first" && exit 1)

up-ingestion: env-check
	$(COMPOSE) --profile ingestion up -d mysql
	$(COMPOSE) ps

up-streaming: env-check
	$(COMPOSE) --profile streaming up -d

up-batch: env-check
	$(COMPOSE) --profile batch up -d

down:
	$(COMPOSE) --profile ingestion --profile streaming --profile batch down

ps:
	$(COMPOSE) ps -a

logs-mysql:
	$(COMPOSE) logs -f mysql

download: env-check
	$(PYTHON) -m data.download_tlc

seed: env-check
	$(PYTHON) -m data.seed_mysql

test:
	$(PYTHON) -m pytest -q

mysql-cli:
	$(COMPOSE) exec mysql mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD $$MYSQL_DATABASE

phase1: up-ingestion download seed test
	@echo "Phase 1 complete. Run the verification checklist in README.md"
