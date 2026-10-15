"""daily_driver_incentive_payout - 01:00: compute payouts for yesterday, then email the HTML report (SMTP via env)."""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

REPO, TO = "/opt/airflow/repo", os.getenv("ALERT_EMAIL_TO", "you@example.com")


def read_report(ds_nodash, ds, **_):
    return Path(f"{REPO}/data/gold/payouts/payout_{ds}.html").read_text()


with DAG("daily_driver_incentive_payout", start_date=datetime(2026, 10, 1), schedule="0 1 * * *", catchup=False,
         default_args={"email": [TO], "email_on_failure": True, "retries": 2, "retry_delay": timedelta(minutes=5)}, tags=["ridehail"]) as dag:
    compute = BashOperator(task_id="compute_payouts",
        bash_command=f"cd {REPO} && PYTHONPATH={REPO} python -m processing.gold.driver_payout --date {{{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}}}")
    render = PythonOperator(task_id="render_report", python_callable=lambda **c: Path(
        f"{REPO}/data/gold/payouts/payout_{(c['data_interval_end'] - timedelta(days=1)).strftime('%Y-%m-%d')}.html").read_text())
    mail = EmailOperator(task_id="email_report", to=TO, subject="Driver incentive payout report", html_content="{{ ti.xcom_pull(task_ids='render_report') }}")
    compute >> render >> mail
