"""Gold layer fraud detection rules engine using Spark SQL window functions."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fraud_detection.config import AppConfig, FraudRulesConfig

RULE_COLUMNS = [
    "rule_1_amount_anomaly",
    "rule_2_velocity_burst",
    "rule_3_impossible_travel",
    "rule_4_intl_after_domestic",
    "rule_5_high_risk_merchant",
    "rule_6_multiple_declined",
    "rule_7_night_spending",
    "rule_8_device_change",
    "rule_9_post_reset_withdrawal",
    "rule_10_velocity_fraud",
]

RULE_WEIGHTS = {
    "rule_1_amount_anomaly": 15,
    "rule_2_velocity_burst": 20,
    "rule_3_impossible_travel": 25,
    "rule_4_intl_after_domestic": 15,
    "rule_5_high_risk_merchant": 10,
    "rule_6_multiple_declined": 15,
    "rule_7_night_spending": 10,
    "rule_8_device_change": 15,
    "rule_9_post_reset_withdrawal": 20,
    "rule_10_velocity_fraud": 20,
}

FRAUD_SCORE_THRESHOLD = 25


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load Silver transactions partitioned by date."""
    return spark.read.format("delta").load(silver_path)


def apply_rule_1_amount_anomaly(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 1: Transaction amount exceeds customer average by 5x."""
    customer_window = Window.partitionBy("customer_id")
    return df.withColumn(
        "rule_1_amount_anomaly",
        F.col("amount")
        > F.avg("amount").over(customer_window) * F.lit(rules.amount_multiplier_threshold),
    )


def apply_rule_2_velocity_burst(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 2: More than 5 transactions within 2 minutes."""
    window_spec = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(0, rules.velocity_window_minutes * 60)
    )
    return df.withColumn(
        "rule_2_velocity_burst",
        F.count("transaction_id").over(window_spec) > rules.velocity_transaction_count,
    )


def apply_rule_3_impossible_travel(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 3: Impossible travel (e.g., NY to London within 30 minutes)."""
    customer_window = Window.partitionBy("customer_id").orderBy("timestamp")

    df_with_lag = (
        df.withColumn("prev_city", F.lag("city").over(customer_window))
        .withColumn("prev_timestamp", F.lag("timestamp").over(customer_window))
        .withColumn(
            "minutes_since_prev",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 60,
        )
    )

    impossible_pairs = [
        ("New York", "London"),
        ("London", "New York"),
        ("Los Angeles", "Tokyo"),
        ("Tokyo", "Los Angeles"),
        ("Chicago", "Dubai"),
        ("Dubai", "Chicago"),
    ]

    condition = F.lit(False)
    for from_city, to_city in impossible_pairs:
        condition = condition | (
            (F.col("prev_city") == from_city)
            & (F.col("city") == to_city)
            & (F.col("minutes_since_prev") <= rules.impossible_travel_minutes)
        )

    return df_with_lag.withColumn("rule_3_impossible_travel", condition).drop(
        "prev_city", "prev_timestamp", "minutes_since_prev"
    )


def apply_rule_4_intl_after_domestic(df: DataFrame) -> DataFrame:
    """Rule 4: International transaction immediately after domestic."""
    customer_window = Window.partitionBy("customer_id").orderBy("timestamp")

    return (
        df.withColumn("prev_is_domestic", F.lag("is_domestic").over(customer_window))
        .withColumn(
            "prev_timestamp",
            F.lag("timestamp").over(customer_window),
        )
        .withColumn(
            "minutes_since_prev",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 60,
        )
        .withColumn(
            "rule_4_intl_after_domestic",
            F.col("prev_is_domestic")
            & (~F.col("is_domestic"))
            & (F.col("minutes_since_prev") <= 5),
        )
        .drop("prev_is_domestic", "prev_timestamp", "minutes_since_prev")
    )


def apply_rule_5_high_risk_merchant(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 5: High-risk merchant category (broadcast join on lookup)."""
    spark = df.sparkSession
    lookup = spark.createDataFrame(
        [(c.lower(),) for c in rules.high_risk_categories],
        ["high_risk_category"],
    )
    from pyspark.sql.functions import broadcast

    return (
        df.join(
            broadcast(lookup),
            F.lower(F.col("merchant_category")) == F.col("high_risk_category"),
            "left",
        )
        .withColumn("rule_5_high_risk_merchant", F.col("high_risk_category").isNotNull())
        .drop("high_risk_category")
    )


def apply_rule_6_multiple_declined(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 6: Multiple declined transactions within 1 hour."""
    declined_window = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-3600, 0)
    )
    return df.withColumn(
        "rule_6_multiple_declined",
        F.sum(
            F.when(F.col("transaction_status") == "declined", 1).otherwise(0)
        ).over(declined_window)
        >= rules.declined_threshold,
    )


def apply_rule_7_night_spending(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 7: Unusual spending during night hours (00:00-05:00)."""
    customer_window = Window.partitionBy("customer_id")
    avg_amount = F.avg("amount").over(customer_window)

    is_night = (F.col("transaction_hour") >= rules.night_start_hour) & (
        F.col("transaction_hour") < rules.night_end_hour
    )

    return df.withColumn(
        "rule_7_night_spending",
        is_night & (F.col("amount") > avg_amount * 3),
    )


def apply_rule_8_device_change(df: DataFrame) -> DataFrame:
    """Rule 8: Device changed within the same session."""
    session_window = Window.partitionBy("customer_id", "session_id").orderBy("timestamp")

    return (
        df.withColumn("prev_device", F.lag("device_id").over(session_window))
        .withColumn(
            "rule_8_device_change",
            F.col("prev_device").isNotNull() & (F.col("device_id") != F.col("prev_device")),
        )
        .drop("prev_device")
    )


def apply_rule_9_post_reset_withdrawal(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 9: Large withdrawal shortly after password reset."""
    customer_window = Window.partitionBy("customer_id").orderBy("timestamp")

    return (
        df.withColumn(
            "prev_reset",
            F.lag("password_reset_flag").over(customer_window),
        )
        .withColumn(
            "prev_timestamp",
            F.lag("timestamp").over(customer_window),
        )
        .withColumn(
            "hours_since_reset",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 3600,
        )
        .withColumn(
            "rule_9_post_reset_withdrawal",
            F.col("prev_reset")
            & (F.col("amount") >= rules.large_withdrawal_threshold)
            & (F.col("hours_since_reset") <= 2),
        )
        .drop("prev_reset", "prev_timestamp", "hours_since_reset")
    )


def apply_rule_10_velocity_fraud(df: DataFrame, rules: FraudRulesConfig) -> DataFrame:
    """Rule 10: 24-hour velocity fraud (high count and amount)."""
    velocity_window = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-86400, 0)
    )

    return df.withColumn(
        "rule_10_velocity_fraud",
        (
            F.sum("amount").over(velocity_window)
            >= rules.velocity_24h_amount_threshold
        )
        & (
            F.count("transaction_id").over(velocity_window)
            >= rules.velocity_24h_count_threshold
        ),
    )


def compute_fraud_score(df: DataFrame) -> DataFrame:
    """Compute weighted fraud score and triggered rules list."""
    score_expr = F.lit(0)
    for rule_col, weight in RULE_WEIGHTS.items():
        score_expr = score_expr + F.when(F.col(rule_col), weight).otherwise(0)

    triggered = F.concat_ws(
        ",",
        *[
            F.when(F.col(c), F.lit(c.replace("rule_", "R").replace("_", ""))).otherwise(F.lit(None))
            for c in RULE_COLUMNS
        ],
    )

    return (
        df.withColumn("fraud_score", score_expr)
        .withColumn("triggered_rules", triggered)
        .withColumn("is_fraudulent", F.col("fraud_score") >= FRAUD_SCORE_THRESHOLD)
        .withColumn("scored_at", F.lit(datetime.utcnow()))
    )


def apply_all_fraud_rules(df: DataFrame, config: AppConfig) -> DataFrame:
    """Apply all 10 fraud detection rules sequentially."""
    rules = config.fraud_rules

    result = apply_rule_1_amount_anomaly(df, rules)
    result = apply_rule_2_velocity_burst(result, rules)
    result = apply_rule_3_impossible_travel(result, rules)
    result = apply_rule_4_intl_after_domestic(result)
    result = apply_rule_5_high_risk_merchant(result, rules)
    result = apply_rule_6_multiple_declined(result, rules)
    result = apply_rule_7_night_spending(result, rules)
    result = apply_rule_8_device_change(result)
    result = apply_rule_9_post_reset_withdrawal(result, rules)
    result = apply_rule_10_velocity_fraud(result, rules)
    result = compute_fraud_score(result)

    return result.select(
        "transaction_id",
        "customer_id",
        "account_id",
        "timestamp",
        "amount",
        "merchant",
        "merchant_category",
        "city",
        "country",
        "device_id",
        *RULE_COLUMNS,
        "fraud_score",
        "is_fraudulent",
        "triggered_rules",
        "scored_at",
        "transaction_date",
    )


def score_transactions(spark: SparkSession, config: AppConfig, logger) -> DataFrame:
    """Load Silver data and apply fraud scoring."""
    silver_df = read_silver(spark, config.paths.silver)
    scored = apply_all_fraud_rules(silver_df, config)

    fraud_count = scored.filter(F.col("is_fraudulent")).count()
    total = scored.count()
    logger.info(
        "fraud_scoring_complete",
        total_transactions=total,
        fraudulent=fraud_count,
        fraud_rate=round(fraud_count / total * 100, 2) if total else 0,
    )
    return scored
