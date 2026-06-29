"""Transaction schema definitions for medallion architecture layers."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("timestamp", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("account_balance", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("password_reset_flag", StringType(), True),
        StructField("session_id", StringType(), True),
    ]
)

SILVER_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("merchant", StringType(), True),
        StructField("merchant_category", StringType(), False),
        StructField("city", StringType(), True),
        StructField("country", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("payment_type", StringType(), False),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("account_balance", DoubleType(), True),
        StructField("transaction_status", StringType(), False),
        StructField("password_reset_flag", BooleanType(), False),
        StructField("session_id", StringType(), True),
        StructField("transaction_date", DateType(), False),
        StructField("transaction_hour", IntegerType(), False),
        StructField("is_domestic", BooleanType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)

GOLD_FRAUD_FLAGS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("amount", DoubleType(), False),
        StructField("merchant", StringType(), True),
        StructField("merchant_category", StringType(), False),
        StructField("city", StringType(), True),
        StructField("country", StringType(), False),
        StructField("device_id", StringType(), True),
        StructField("rule_1_amount_anomaly", BooleanType(), False),
        StructField("rule_2_velocity_burst", BooleanType(), False),
        StructField("rule_3_impossible_travel", BooleanType(), False),
        StructField("rule_4_intl_after_domestic", BooleanType(), False),
        StructField("rule_5_high_risk_merchant", BooleanType(), False),
        StructField("rule_6_multiple_declined", BooleanType(), False),
        StructField("rule_7_night_spending", BooleanType(), False),
        StructField("rule_8_device_change", BooleanType(), False),
        StructField("rule_9_post_reset_withdrawal", BooleanType(), False),
        StructField("rule_10_velocity_fraud", BooleanType(), False),
        StructField("fraud_score", IntegerType(), False),
        StructField("is_fraudulent", BooleanType(), False),
        StructField("triggered_rules", StringType(), True),
        StructField("scored_at", TimestampType(), False),
    ]
)
