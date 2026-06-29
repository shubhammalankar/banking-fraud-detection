"""Tests for Silver layer transformations."""

from datetime import datetime

from pyspark.sql import functions as F

from fraud_detection.silver.transform import (
    handle_nulls,
    remove_duplicates,
    transform_to_silver,
    validate_and_cast,
)


def test_validate_and_cast_filters_invalid_amount(spark):
    data = [
        ("TX1", "ACC1", "C1", "2024-01-01T10:00:00", "-50", "Store", "groceries",
         "NY", "US", "40.7", "-74.0", "credit_card", "D1", "1.1.1.1", "1000",
         "approved", "false", "S1", "2024-01-01", "file.csv"),
    ]
    cols = [
        "transaction_id", "account_id", "customer_id", "timestamp", "amount",
        "merchant", "merchant_category", "city", "country", "latitude", "longitude",
        "payment_type", "device_id", "ip_address", "account_balance",
        "transaction_status", "password_reset_flag", "session_id",
        "ingestion_timestamp", "source_file",
    ]
    df = spark.createDataFrame(data, cols)
    result = validate_and_cast(df)
    assert result.count() == 0


def test_handle_nulls_fills_merchant(spark):
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("transaction_id", StringType()),
            StructField("account_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("amount", DoubleType()),
            StructField("merchant", StringType()),
            StructField("merchant_category", StringType()),
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("payment_type", StringType()),
            StructField("device_id", StringType()),
            StructField("ip_address", StringType()),
            StructField("account_balance", DoubleType()),
            StructField("transaction_status", StringType()),
            StructField("password_reset_flag", BooleanType()),
            StructField("session_id", StringType()),
        ]
    )
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 1, 10), 100.0, None, "groceries",
         "NY", "US", 40.7, -74.0, "credit_card", None, None, 1000.0,
         "approved", False, "S1"),
    ]
    df = spark.createDataFrame(data, schema)
    result = handle_nulls(df)
    row = result.collect()[0]
    assert row["merchant"] == "UNKNOWN"
    assert row["device_id"] == "UNKNOWN"


def test_remove_duplicates_keeps_latest(spark):
    data = [
        ("TX1", "ACC1", "C1", "2024-01-01T10:00:00", "100", "Store", "groceries",
         "NY", "US", "40.7", "-74.0", "credit_card", "D1", "1.1.1.1", "1000",
         "approved", "false", "S1", "2024-01-01T08:00:00", "old.csv", "2024-01-01"),
        ("TX1", "ACC1", "C1", "2024-01-01T10:00:00", "100", "Store", "groceries",
         "NY", "US", "40.7", "-74.0", "credit_card", "D1", "1.1.1.1", "1000",
         "approved", "false", "S1", "2024-01-01T12:00:00", "new.csv", "2024-01-01"),
    ]
    cols = [
        "transaction_id", "account_id", "customer_id", "timestamp", "amount",
        "merchant", "merchant_category", "city", "country", "latitude", "longitude",
        "payment_type", "device_id", "ip_address", "account_balance",
        "transaction_status", "password_reset_flag", "session_id",
        "ingestion_timestamp", "source_file", "ingestion_date",
    ]
    df = spark.createDataFrame(data, cols)
    result = remove_duplicates(df)
    assert result.count() == 1


def test_transform_to_silver_schema(spark):
    data = [
        ("TX1", "ACC1", "C1", "2024-01-01T10:00:00", "150.50", "Store", "groceries",
         "New York", "US", "40.7128", "-74.0060", "credit_card", "DEV1", "10.0.0.1",
         "5000.00", "approved", "false", "SES1", "2024-01-01T12:00:00", "file.csv", "2024-01-01"),
    ]
    cols = [
        "transaction_id", "account_id", "customer_id", "timestamp", "amount",
        "merchant", "merchant_category", "city", "country", "latitude", "longitude",
        "payment_type", "device_id", "ip_address", "account_balance",
        "transaction_status", "password_reset_flag", "session_id",
        "ingestion_timestamp", "source_file", "ingestion_date",
    ]
    df = spark.createDataFrame(data, cols)
    result = transform_to_silver(df)
    row = result.collect()[0]
    assert row["amount"] == 150.50
    assert row["is_domestic"] is True
    assert row["transaction_hour"] == 10
