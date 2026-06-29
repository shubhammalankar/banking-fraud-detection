"""
Airflow DAG: Banking Fraud Detection Pipeline

Schedule: Daily at 02:00 UTC
Owner: Fraud Analytics Engineering
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/app/src")

default_args = {
    "owner": "fraud-analytics-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

PYTHON = "python"
PIPELINE = "-m fraud_detection.etl.pipeline"
GENERATOR = "-m fraud_detection.data_generation.generator"


def validate_pipeline_output(**context):
    """Post-run validation hook for monitoring."""
    from fraud_detection.config import load_config
    from fraud_detection.spark_session import create_spark_session

    config = load_config()
    spark = create_spark_session(config)
    try:
        fraud_rate = spark.read.format("delta").load(f"{config.paths.gold}/fraud_rate")
        row = fraud_rate.collect()[0]
        rate = row["fraud_rate_pct"]
        if rate > 50:
            raise ValueError(f"Anomaly: fraud rate {rate}% exceeds 50% threshold")
        context["ti"].xcom_push(key="fraud_rate_pct", value=rate)
    finally:
        spark.stop()


with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Daily medallion fraud detection batch pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "spark", "delta", "production"],
    doc_md=__doc__,
) as dag_daily:

    generate_data = BashOperator(
        task_id="generate_transaction_data",
        bash_command=f"{PYTHON} {GENERATOR}",
        env={"PYTHONPATH": "/app/src"},
    )

    run_bronze_silver_gold = BashOperator(
        task_id="run_medallion_pipeline",
        bash_command=f"{PYTHON} {PIPELINE}",
        env={
            "PYTHONPATH": "/app/src",
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "fraud_analytics"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "fraud_user"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "fraud_pass"),
        },
    )

    validate_output = PythonOperator(
        task_id="validate_fraud_rate",
        python_callable=validate_pipeline_output,
    )

    generate_data >> run_bronze_silver_gold >> validate_output


with DAG(
    dag_id="fraud_detection_incremental",
    default_args=default_args,
    description="Incremental fraud detection every 4 hours",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "spark", "incremental"],
) as dag_incremental:

    incremental_run = BashOperator(
        task_id="run_incremental_pipeline",
        bash_command=f"{PYTHON} {PIPELINE} --skip-postgres",
        env={"PYTHONPATH": "/app/src"},
    )
