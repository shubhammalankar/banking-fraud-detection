"""Tests for fraud detection rules engine."""

from datetime import datetime

from pyspark.sql import functions as F

from fraud_detection.config import AppConfig, FraudRulesConfig
from fraud_detection.gold.fraud_rules import (
    FRAUD_SCORE_THRESHOLD,
    apply_all_fraud_rules,
    apply_rule_3_impossible_travel,
    apply_rule_5_high_risk_merchant,
    apply_rule_6_multiple_declined,
    apply_rule_9_post_reset_withdrawal,
    compute_fraud_score,
)


def _make_config():
    return AppConfig(fraud_rules=FraudRulesConfig())


def test_rule_3_impossible_travel(spark):
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 15, 10, 0), 100.0,
         "Store", "groceries", "New York", "US", 40.71, -74.01,
         "credit_card", "DEV1", "1.1.1.1", 5000.0, "approved", False, "S1"),
        ("TX2", "ACC1", "C1", datetime(2024, 1, 15, 10, 20), 200.0,
         "Store", "groceries", "London", "GB", 51.51, -0.13,
         "credit_card", "DEV1", "1.1.1.2", 4800.0, "approved", False, "S1"),
    ]
    df = _enrich(spark, data)
    result = apply_rule_3_impossible_travel(df, FraudRulesConfig())
    rows = {r["transaction_id"]: r["rule_3_impossible_travel"] for r in result.collect()}
    assert rows["TX1"] in (False, None)
    assert rows["TX2"] is True


def test_rule_5_high_risk_merchant(spark):
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 15, 10, 0), 500.0,
         "Casino", "gambling", "Vegas", "US", 36.17, -115.14,
         "credit_card", "DEV1", "1.1.1.1", 5000.0, "approved", False, "S1"),
        ("TX2", "ACC1", "C1", datetime(2024, 1, 15, 11, 0), 50.0,
         "Grocery", "groceries", "Chicago", "US", 41.88, -87.63,
         "debit_card", "DEV1", "1.1.1.2", 4500.0, "approved", False, "S1"),
    ]
    df = _enrich(spark, data)
    result = apply_rule_5_high_risk_merchant(df, FraudRulesConfig())
    rows = {r["transaction_id"]: r["rule_5_high_risk_merchant"] for r in result.collect()}
    assert rows["TX1"] is True
    assert rows["TX2"] is False


def test_rule_6_multiple_declined(spark):
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 15, 14, 0), 50.0,
         "Store", "groceries", "LA", "US", 34.05, -118.24,
         "credit_card", "DEV1", "1.1.1.1", 3000.0, "declined", False, "S1"),
        ("TX2", "ACC1", "C1", datetime(2024, 1, 15, 14, 5), 50.0,
         "Store", "groceries", "LA", "US", 34.05, -118.24,
         "credit_card", "DEV1", "1.1.1.2", 2950.0, "declined", False, "S1"),
        ("TX3", "ACC1", "C1", datetime(2024, 1, 15, 14, 10), 50.0,
         "Store", "groceries", "LA", "US", 34.05, -118.24,
         "credit_card", "DEV1", "1.1.1.3", 2900.0, "declined", False, "S1"),
    ]
    df = _enrich(spark, data)
    result = apply_rule_6_multiple_declined(df, FraudRulesConfig())
    flagged = [r for r in result.collect() if r["rule_6_multiple_declined"]]
    assert len(flagged) >= 1


def test_rule_9_post_reset_withdrawal(spark):
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 15, 8, 0), 100.0,
         "Store", "retail", "NY", "US", 40.71, -74.01,
         "credit_card", "DEV1", "1.1.1.1", 8000.0, "approved", True, "S1"),
        ("TX2", "ACC1", "C1", datetime(2024, 1, 15, 8, 30), 7000.0,
         "Transfer", "money_transfer", "NY", "US", 40.71, -74.01,
         "wire_transfer", "DEV1", "1.1.1.2", 1000.0, "approved", False, "S1"),
    ]
    df = _enrich(spark, data)
    result = apply_rule_9_post_reset_withdrawal(df, FraudRulesConfig())
    rows = {r["transaction_id"]: r["rule_9_post_reset_withdrawal"] for r in result.collect()}
    assert rows["TX2"] is True


def test_fraud_score_threshold():
    assert FRAUD_SCORE_THRESHOLD == 25


def test_compute_fraud_score(spark):
    data = [
        ("TX1", "ACC1", "C1", datetime(2024, 1, 15, 10, 0), 100.0,
         "Store", "gambling", "NY", "US", 40.71, -74.01,
         "credit_card", "DEV1", "1.1.1.1", 5000.0, "approved", False, "S1"),
    ]
    df = _enrich(spark, data)
    df = df.withColumn("rule_5_high_risk_merchant", F.lit(True))
    for col in [
        "rule_1_amount_anomaly", "rule_2_velocity_burst", "rule_3_impossible_travel",
        "rule_4_intl_after_domestic", "rule_6_multiple_declined", "rule_7_night_spending",
        "rule_8_device_change", "rule_9_post_reset_withdrawal", "rule_10_velocity_fraud",
    ]:
        df = df.withColumn(col, F.lit(False))
    result = compute_fraud_score(df)
    row = result.collect()[0]
    assert row["fraud_score"] == 10
    assert row["is_fraudulent"] is False


def test_apply_all_fraud_rules(sample_transactions):
    config = _make_config()
    result = apply_all_fraud_rules(sample_transactions, config)
    assert "fraud_score" in result.columns
    assert "is_fraudulent" in result.columns
    assert result.count() == sample_transactions.count()


def _enrich(spark, data):
    cols = [
        "transaction_id", "account_id", "customer_id", "timestamp", "amount",
        "merchant", "merchant_category", "city", "country", "latitude", "longitude",
        "payment_type", "device_id", "ip_address", "account_balance",
        "transaction_status", "password_reset_flag", "session_id",
    ]
    df = spark.createDataFrame(data, cols)
    return (
        df.withColumn("transaction_date", F.to_date("timestamp"))
        .withColumn("transaction_hour", F.hour("timestamp"))
        .withColumn("is_domestic", F.col("country") == "US")
        .withColumn("processed_at", F.current_timestamp())
    )
