"""Gold layer analytics: fraud KPIs, rankings, and trend aggregations."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fraud_detection.config import AppConfig


def write_gold_table(df: DataFrame, path: str, partition_col: str | None = None) -> None:
    """Write analytics DataFrame to Gold Delta table."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    writer = df.write.format("delta").mode("overwrite")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def build_fraud_scores(scored_df: DataFrame) -> DataFrame:
    """Gold: Individual transaction fraud scores (primary fact table)."""
    return scored_df


def build_fraud_rate(scored_df: DataFrame) -> DataFrame:
    """Gold: Overall fraud rate KPI."""
    total = scored_df.count()
    fraud = scored_df.filter(F.col("is_fraudulent")).count()
    return scored_df.sparkSession.createDataFrame(
        [(total, fraud, round(fraud / total * 100, 4) if total else 0.0)],
        ["total_transactions", "fraudulent_transactions", "fraud_rate_pct"],
    )


def build_fraud_by_country(scored_df: DataFrame) -> DataFrame:
    """Gold: Fraud aggregation by country."""
    return (
        scored_df.groupBy("country")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.avg("fraud_score").alias("avg_fraud_score"),
            F.sum(F.when(F.col("is_fraudulent"), F.col("amount")).otherwise(0)).alias(
                "fraud_amount"
            ),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 4),
        )
        .orderBy(F.col("fraud_rate_pct").desc())
    )


def build_fraud_by_merchant(scored_df: DataFrame) -> DataFrame:
    """Gold: Fraud aggregation by merchant."""
    return (
        scored_df.groupBy("merchant", "merchant_category")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.avg("amount").alias("avg_amount"),
            F.max("fraud_score").alias("max_fraud_score"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 4),
        )
        .orderBy(F.col("fraud_count").desc())
        .limit(100)
    )


def build_fraud_by_device(scored_df: DataFrame) -> DataFrame:
    """Gold: Fraud aggregation by device."""
    return (
        scored_df.groupBy("device_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.avg("fraud_score").alias("avg_fraud_score"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 4),
        )
        .filter(F.col("fraud_count") > 0)
        .orderBy(F.col("fraud_count").desc())
        .limit(100)
    )


def build_daily_fraud_trend(scored_df: DataFrame) -> DataFrame:
    """Gold: Daily fraud trend for time-series dashboards."""
    daily = (
        scored_df.groupBy("transaction_date")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraudulent"), F.col("amount")).otherwise(0)).alias(
                "fraud_amount"
            ),
            F.avg("fraud_score").alias("avg_fraud_score"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 4),
        )
    )

    trend_window = Window.orderBy("transaction_date")
    return (
        daily.withColumn("prev_day_fraud_rate", F.lag("fraud_rate_pct").over(trend_window))
        .withColumn("next_day_fraud_rate", F.lead("fraud_rate_pct").over(trend_window))
        .withColumn(
            "day_over_day_change",
            F.col("fraud_rate_pct") - F.col("prev_day_fraud_rate"),
        )
        .orderBy("transaction_date")
    )


def build_customer_risk_ranking(scored_df: DataFrame) -> DataFrame:
    """Gold: Customer risk ranking using dense_rank window function."""
    customer_agg = (
        scored_df.groupBy("customer_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.max("fraud_score").alias("max_fraud_score"),
            F.avg("fraud_score").alias("avg_fraud_score"),
            F.sum(F.when(F.col("is_fraudulent"), F.col("amount")).otherwise(0)).alias(
                "fraud_exposure"
            ),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("fraud_count") / F.col("total_transactions") * 100, 4),
        )
    )

    risk_window = Window.orderBy(
        F.col("max_fraud_score").desc(),
        F.col("fraud_count").desc(),
    )

    return (
        customer_agg.withColumn("risk_rank", F.dense_rank().over(risk_window))
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_rank") <= 10, "CRITICAL")
            .when(F.col("risk_rank") <= 50, "HIGH")
            .when(F.col("risk_rank") <= 200, "MEDIUM")
            .otherwise("LOW"),
        )
        .orderBy("risk_rank")
    )


def build_top_suspicious_accounts(scored_df: DataFrame) -> DataFrame:
    """Gold: Top suspicious accounts ranked by fraud score."""
    account_agg = (
        scored_df.groupBy("account_id", "customer_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraudulent"), 1).otherwise(0)).alias("fraud_count"),
            F.max("fraud_score").alias("max_fraud_score"),
            F.sum("amount").alias("total_amount"),
            F.max("timestamp").alias("last_transaction_at"),
            F.collect_set("triggered_rules").alias("all_triggered_rules"),
        )
        .filter(F.col("fraud_count") > 0)
    )

    account_window = Window.orderBy(
        F.col("max_fraud_score").desc(),
        F.col("fraud_count").desc(),
    )

    return (
        account_agg.withColumn("suspicion_rank", F.dense_rank().over(account_window))
        .withColumn(
            "suspicion_level",
            F.when(F.col("max_fraud_score") >= 60, "SEVERE")
            .when(F.col("max_fraud_score") >= 40, "HIGH")
            .when(F.col("max_fraud_score") >= 25, "ELEVATED")
            .otherwise("MODERATE"),
        )
        .orderBy("suspicion_rank")
        .limit(50)
    )


def build_rule_trigger_summary(scored_df: DataFrame) -> DataFrame:
    """Gold: Summary of which rules trigger most frequently."""
    rule_cols = [c for c in scored_df.columns if c.startswith("rule_")]
    rows = []
    for rule in rule_cols:
        count = scored_df.filter(F.col(rule)).count()
        rows.append((rule, count))

    return scored_df.sparkSession.createDataFrame(rows, ["rule_name", "trigger_count"]).orderBy(
        F.col("trigger_count").desc()
    )


def run_gold_analytics(
    spark: SparkSession,
    scored_df: DataFrame,
    config: AppConfig,
    logger,
) -> dict[str, int]:
    """Build and persist all Gold layer analytics tables."""
    gold_base = config.paths.gold
    results: dict[str, int] = {}

    tables = {
        "fraud_scores": (build_fraud_scores(scored_df), "transaction_date"),
        "fraud_rate": (build_fraud_rate(scored_df), None),
        "fraud_by_country": (build_fraud_by_country(scored_df), None),
        "fraud_by_merchant": (build_fraud_by_merchant(scored_df), None),
        "fraud_by_device": (build_fraud_by_device(scored_df), None),
        "daily_fraud_trend": (build_daily_fraud_trend(scored_df), "transaction_date"),
        "customer_risk_ranking": (build_customer_risk_ranking(scored_df), None),
        "top_suspicious_accounts": (build_top_suspicious_accounts(scored_df), None),
        "rule_trigger_summary": (build_rule_trigger_summary(scored_df), None),
    }

    for name, (df, partition) in tables.items():
        path = f"{gold_base}/{name}"
        write_gold_table(df, path, partition)
        count = df.count()
        results[name] = count
        logger.info("gold_table_written", table=name, records=count, path=path)

        if name == "fraud_by_country":
            df.explain(mode="formatted")
            logger.info("explain_plan_logged", table=name)

    return results
