"""master_batch_dag - daily 00:30. Runs the repo's code (mounted at /opt/airflow/repo) so DAG parsing needs no Spark imports."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

RUN = "cd /opt/airflow/repo && PYTHONPATH=/opt/airflow/repo "
default_args = {"owner": "data_engineering", "depends_on_past": False, "email_on_failure": True, "retries": 2,
                "retry_delay": timedelta(minutes=5)}

with DAG("master_batch_dag", default_args=default_args, start_date=datetime(2026, 10, 1), schedule="30 0 * * *", catchup=False,
         tags=["ridehail"]) as dag:
    ingest = BashOperator(task_id="mysql_to_raw_parquet", bash_command=RUN + "python -m ingestion.sqoop.spark_jdbc_import")
    clean = BashOperator(task_id="spark_core_cleansing", bash_command=RUN + "python -m processing.batch.spark_core_trips")
    star = BashOperator(task_id="build_hive_star_schema", bash_command=RUN + "python -m storage.build_star_schema")
    s3 = BashOperator(task_id="sync_s3_and_glue", bash_command=RUN + "python -m lakehouse.s3_sync --crawler")
    hourly = TriggerDagRunOperator(task_id="trigger_hourly_zone_agg", trigger_dag_id="hourly_zone_aggregation")
    payout = TriggerDagRunOperator(task_id="trigger_driver_payout", trigger_dag_id="daily_driver_incentive_payout")
    ingest >> clean >> star >> s3 >> [hourly, payout]
