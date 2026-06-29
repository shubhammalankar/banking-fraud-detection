"""Pytest fixtures for fraud detection pipeline tests."""

from __future__ import annotations

import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from fraud_detection.config import AppConfig, FraudRulesConfig, PathsConfig, SparkConfig


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta Lake for testing."""
    import os

    from delta import configure_spark_with_delta_pip

    warehouse = tempfile.mkdtemp(prefix="spark_test_")
    os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", "python")
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", "python")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("fraud-detection-tests")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def test_config(tmp_path):
    """Provide test configuration with isolated temp paths."""
    base = tmp_path / "data"
    return AppConfig(
        spark=SparkConfig(app_name="test", master="local[2]", shuffle_partitions=4),
        paths=PathsConfig(
            raw_data=str(base / "raw"),
            bronze=str(base / "bronze"),
            silver=str(base / "silver"),
            gold=str(base / "gold"),
            checkpoints=str(base / "checkpoints"),
        ),
        fraud_rules=FraudRulesConfig(),
        log_level="ERROR",
        num_records=500,
    )


@pytest.fixture
def sample_transactions(spark):
    """Create a small sample transaction DataFrame for rule testing."""
    data = [
        ("TX001", "ACC001", "CUST001", datetime(2024, 1, 15, 10, 0), 100.0,
         "Store A", "groceries", "New York", "US", 40.71, -74.01,
         "credit_card", "DEV001", "1.2.3.4", 5000.0, "approved", False, "SES001"),
        ("TX002", "ACC001", "CUST001", datetime(2024, 1, 15, 10, 20), 800.0,
         "Store B", "gambling", "London", "GB", 51.51, -0.13,
         "credit_card", "DEV001", "1.2.3.5", 4200.0, "approved", False, "SES001"),
        ("TX003", "ACC002", "CUST002", datetime(2024, 1, 15, 2, 30), 5000.0,
         "Store C", "retail", "Chicago", "US", 41.88, -87.63,
         "debit_card", "DEV002", "2.3.4.5", 10000.0, "approved", False, "SES002"),
        ("TX004", "ACC003", "CUST003", datetime(2024, 1, 15, 14, 0), 50.0,
         "Store D", "groceries", "Los Angeles", "US", 34.05, -118.24,
         "credit_card", "DEV003", "3.4.5.6", 3000.0, "declined", False, "SES003"),
        ("TX005", "ACC003", "CUST003", datetime(2024, 1, 15, 14, 5), 50.0,
         "Store E", "groceries", "Los Angeles", "US", 34.05, -118.24,
         "credit_card", "DEV003", "3.4.5.7", 2950.0, "declined", False, "SES003"),
        ("TX006", "ACC003", "CUST003", datetime(2024, 1, 15, 14, 10), 50.0,
         "Store F", "groceries", "Los Angeles", "US", 34.05, -118.24,
         "credit_card", "DEV003", "3.4.5.8", 2900.0, "declined", False, "SES003"),
        ("TX007", "ACC004", "CUST004", datetime(2024, 1, 15, 8, 0), 100.0,
         "Store G", "retail", "New York", "US", 40.71, -74.01,
         "credit_card", "DEV004", "4.5.6.7", 8000.0, "approved", True, "SES004"),
        ("TX008", "ACC004", "CUST004", datetime(2024, 1, 15, 8, 30), 7000.0,
         "Transfer Co", "money_transfer", "New York", "US", 40.71, -74.01,
         "wire_transfer", "DEV004", "4.5.6.8", 1000.0, "approved", False, "SES004"),
    ]
    columns = [
        "transaction_id", "account_id", "customer_id", "timestamp", "amount",
        "merchant", "merchant_category", "city", "country", "latitude", "longitude",
        "payment_type", "device_id", "ip_address", "account_balance",
        "transaction_status", "password_reset_flag", "session_id",
    ]
    from pyspark.sql import functions as F

    df = spark.createDataFrame(data, columns)
    return (
        df.withColumn("transaction_date", F.to_date("timestamp"))
        .withColumn("transaction_hour", F.hour("timestamp"))
        .withColumn("is_domestic", F.col("country") == "US")
        .withColumn("processed_at", F.current_timestamp())
    )
