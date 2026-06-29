"""Spark session factory with Delta Lake and performance tuning."""

from __future__ import annotations

from pyspark.sql import SparkSession

from fraud_detection.config import AppConfig


def create_spark_session(config: AppConfig) -> SparkSession:
    """Create a production-tuned SparkSession with Delta Lake support."""
    from delta import configure_spark_with_delta_pip

    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()

    builder = (
        SparkSession.builder.appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", str(config.spark.adaptive_query_execution).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(config.spark.broadcast_threshold))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if config.environment == "development":
        builder = (
            builder.config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.ui.showConsoleProgress", "true")
        )

    builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


def log_explain_plan(spark: SparkSession, sql: str, logger) -> None:
    """Log the physical explain plan for query optimization review."""
    plan_df = spark.sql(f"EXPLAIN COST {sql}") if hasattr(spark, "sql") else None
    if plan_df is not None:
        plan = "\n".join(row[0] for row in plan_df.collect())
        logger.info("query_explain_plan", sql=sql[:200], plan=plan[:2000])
    else:
        logger.info("query_explain_plan_skipped", sql=sql[:200])
