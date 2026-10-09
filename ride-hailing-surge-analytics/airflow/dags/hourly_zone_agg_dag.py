"""Hourly Zone Aggregation Airflow DAG.

Schedule: Every hour (0 * * * *)
Aggregates hourly zone-level KPIs (demand, supply, surge, revenue/km, ETA).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="hourly_zone_aggregation_dag",
    default_args=default_args,
    description="Hourly aggregation of zone KPIs and surge statistics",
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:

    aggregate_zone_kpis = BashOperator(
        task_id="aggregate_hourly_zone_kpis",
        bash_command="echo 'Aggregating hourly zone KPIs...'",
    )

    refresh_athena_partitions = BashOperator(
        task_id="refresh_athena_partitions",
        bash_command="echo 'Refreshing Glue/Athena partitions...'",
    )

    aggregate_zone_kpis >> refresh_athena_partitions
