"""PostgreSQL export for Gold analytics serving layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from fraud_detection.config import AppConfig

GOLD_TABLES = [
    "fraud_scores",
    "fraud_rate",
    "fraud_by_country",
    "fraud_by_merchant",
    "fraud_by_device",
    "daily_fraud_trend",
    "customer_risk_ranking",
    "top_suspicious_accounts",
    "rule_trigger_summary",
]


def _jdbc_properties(config: AppConfig) -> dict[str, str]:
    return {
        "user": config.postgres.user,
        "password": config.postgres.password,
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
    }


def export_table_to_postgres(
    df: DataFrame,
    table_name: str,
    config: AppConfig,
    mode: str = "overwrite",
) -> None:
    """Write a Spark DataFrame to PostgreSQL via JDBC."""
    (
        df.write.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", table_name)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def export_gold_to_postgres(
    spark: SparkSession,
    config: AppConfig,
    logger,
) -> dict[str, int]:
    """Export all Gold Delta tables to PostgreSQL."""
    gold_base = config.paths.gold
    results: dict[str, int] = {}

    for table in GOLD_TABLES:
        path = f"{gold_base}/{table}"
        try:
            df = spark.read.format("delta").load(path)
            export_table_to_postgres(df, table, config)
            count = df.count()
            results[table] = count
            logger.info("postgres_export_complete", table=table, records=count)
        except Exception as exc:
            logger.error("postgres_export_failed", table=table, error=str(exc))

    return results


def log_pipeline_run(
    spark: SparkSession,
    config: AppConfig,
    run_id: str,
    status: str,
    layer_stats: dict,
    duration_seconds: float,
) -> None:
    """Write pipeline run metadata to PostgreSQL audit table."""
    audit_df = spark.createDataFrame(
        [(run_id, status, str(layer_stats), duration_seconds)],
        ["run_id", "status", "layer_stats", "duration_seconds"],
    )
    try:
        export_table_to_postgres(audit_df, "pipeline_runs", config, mode="append")
    except Exception:
        pass
