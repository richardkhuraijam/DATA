"""hourly_zone_aggregation - every hour: aggregate zone KPIs, run the DQ gate (task fails on bad data)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

RUN = "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo "
with DAG("hourly_zone_aggregation", start_date=datetime(2026, 10, 1), schedule="@hourly", catchup=False,
         default_args={"email_on_failure": True, "retries": 2, "retry_delay": timedelta(minutes=3)}, tags=["ridehail"]) as dag:
    BashOperator(task_id="aggregate_and_dq_check", bash_command=RUN + "python -m processing.gold.hourly_zone_agg")
