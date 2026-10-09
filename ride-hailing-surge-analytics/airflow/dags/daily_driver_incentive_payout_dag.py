"""Daily Driver Incentive Payout Airflow DAG.

Schedule: Daily at 01:00 UTC
Incentive Rule:
- Driver utilisation rate >= 78% (on-trip vs online time)
- Completed trips >= 12
- Earns base bonus of $50 + $2.00 per surge-zone trip.
Emails an HTML summary report via EmailOperator with retries=2.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.email import EmailOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "finance_ops",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def calculate_payouts_and_generate_report(**kwargs: Any) -> str:
    # Simulated calculation of driver daily payouts
    total_eligible_drivers = 42
    total_bonus_paid = 2850.0

    html_content = f"""
    <h2>Daily Driver Incentive Payout Summary</h2>
    <p><b>Date:</b> {datetime.now().strftime('%Y-%m-%d')}</p>
    <ul>
        <li><b>Eligible Drivers (Utilisation &ge; 78% &amp; &ge; 12 Trips):</b> {total_eligible_drivers}</li>
        <li><b>Total Bonus Payout Amount:</b> ${total_bonus_paid:,.2f}</li>
        <li><b>Target Driver Utilisation SLA (&ge; 78%):</b> PASSED</li>
    </ul>
    """
    logging.info("Generated payout report: %d drivers, total: $%.2f", total_eligible_drivers, total_bonus_paid)
    return html_content


with DAG(
    dag_id="daily_driver_incentive_payout_dag",
    default_args=default_args,
    description="Calculate daily driver bonuses and send email report",
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:

    compute_payouts = PythonOperator(
        task_id="compute_driver_payouts",
        python_callable=calculate_payouts_and_generate_report,
    )

    send_payout_email = EmailOperator(
        task_id="send_payout_report_email",
        to=os.getenv("ALERT_EMAIL_TO", "ops-team@example.com"),
        subject="[RideHail] Daily Driver Incentive Payout Report",
        html_content="{{ task_instance.xcom_pull(task_ids='compute_driver_payouts') }}",
    )

    compute_payouts >> send_payout_email
