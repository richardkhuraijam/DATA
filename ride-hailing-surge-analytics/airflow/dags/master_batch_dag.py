"""Master Batch Airflow DAG.

Schedule: Daily at 00:30 UTC
Tasks:
1. Sqoop / Spark JDBC import from MySQL
2. Spark Core cleansing and pair-RDD analysis
3. Sync Parquet partitions to S3 curated storage
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

from ingestion.sqoop.spark_jdbc_import import main as run_jdbc_import
from processing.batch.spark_core_trips import main as run_batch_analysis

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="master_batch_dag",
    default_args=default_args,
    description="Daily master batch ingestion and cleansing pipeline",
    schedule_interval="30 0 * * *",
    catchup=False,
) as dag:

    ingest_mysql = PythonOperator(
        task_id="ingest_mysql_tables",
        python_callable=run_jdbc_import,
    )

    spark_batch_cleansing = PythonOperator(
        task_id="spark_core_batch_cleansing",
        python_callable=run_batch_analysis,
    )

    sync_s3_curated = BashOperator(
        task_id="sync_s3_curated",
        bash_command="python3 -m lakehouse.s3_sync",
    )

    ingest_mysql >> spark_batch_cleansing >> sync_s3_curated
