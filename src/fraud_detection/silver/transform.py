"""Silver layer: validation, cleansing, deduplication, schema enforcement."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from fraud_detection.config import AppConfig
from fraud_detection.schemas import SILVER_TRANSACTION_SCHEMA

VALID_STATUSES = {"approved", "declined", "pending", "reversed"}
VALID_PAYMENT_TYPES = {"credit_card", "debit_card", "wire_transfer", "ach", "mobile_pay"}
DOMESTIC_COUNTRY = "US"


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read Bronze Delta table."""
    return spark.read.format("delta").load(bronze_path)


def validate_and_cast(df: DataFrame) -> DataFrame:
    """Apply data validation rules and enforce Silver schema types."""
    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("account_balance", F.col("account_balance").cast(DoubleType()))
        .withColumn(
            "password_reset_flag",
            F.when(
                F.lower(F.col("password_reset_flag").cast("string")).isin("true", "1", "yes"),
                True,
            ).otherwise(False),
        )
        .withColumn("transaction_status", F.lower(F.trim(F.col("transaction_status"))))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn("merchant_category", F.lower(F.trim(F.col("merchant_category"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
        .filter(F.col("transaction_status").isin(list(VALID_STATUSES)))
        .filter(F.col("payment_type").isin(list(VALID_PAYMENT_TYPES)))
    )


def handle_nulls(df: DataFrame) -> DataFrame:
    """Impute or flag null values according to business rules."""
    return (
        df.withColumn("merchant", F.coalesce(F.col("merchant"), F.lit("UNKNOWN")))
        .withColumn("merchant_category", F.coalesce(F.col("merchant_category"), F.lit("unknown")))
        .withColumn("city", F.coalesce(F.col("city"), F.lit("UNKNOWN")))
        .withColumn("country", F.coalesce(F.col("country"), F.lit("US")))
        .withColumn("latitude", F.coalesce(F.col("latitude"), F.lit(0.0)))
        .withColumn("longitude", F.coalesce(F.col("longitude"), F.lit(0.0)))
        .withColumn("device_id", F.coalesce(F.col("device_id"), F.lit("UNKNOWN")))
        .withColumn("ip_address", F.coalesce(F.col("ip_address"), F.lit("0.0.0.0")))
        .withColumn("session_id", F.coalesce(F.col("session_id"), F.col("transaction_id")))
        .withColumn(
            "account_balance",
            F.coalesce(F.col("account_balance"), F.lit(0.0)),
        )
    )


def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate transactions keeping the latest ingestion record."""
    window = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc())
    return (
        df.withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank", "ingestion_timestamp", "source_file", "ingestion_date")
    )


def enrich_silver(df: DataFrame) -> DataFrame:
    """Add derived columns for downstream fraud analytics."""
    return (
        df.withColumn("transaction_date", F.to_date("timestamp"))
        .withColumn("transaction_hour", F.hour("timestamp"))
        .withColumn("is_domestic", F.col("country") == F.lit(DOMESTIC_COUNTRY))
        .withColumn("processed_at", F.lit(datetime.utcnow()))
    )


def enforce_schema(df: DataFrame) -> DataFrame:
    """Select and cast columns to match Silver schema contract."""
    schema_cols = [f.name for f in SILVER_TRANSACTION_SCHEMA.fields]
    return df.select(*schema_cols)


def transform_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Full Silver transformation pipeline."""
    return enforce_schema(
        enrich_silver(
            remove_duplicates(
                handle_nulls(
                    validate_and_cast(bronze_df)
                )
            )
        )
    )


def write_silver_incremental(
    spark: SparkSession,
    df: DataFrame,
    silver_path: str,
    merge_key: str = "transaction_id",
) -> int:
    """Merge cleansed records into Silver Delta table."""
    partitioned = df.repartition(F.col("transaction_date"))

    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                partitioned.alias("source"),
                f"target.{merge_key} = source.{merge_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            partitioned.write.format("delta")
            .mode("overwrite")
            .partitionBy("transaction_date")
            .save(silver_path)
        )

    return partitioned.count()


def process_silver(spark: SparkSession, config: AppConfig, logger) -> int:
    """Execute Silver layer transformation pipeline."""
    bronze_path = config.paths.bronze
    silver_path = config.paths.silver

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        logger.warning("silver_process_skipped", reason="bronze_not_found")
        return 0

    bronze_df = read_bronze(spark, bronze_path)
    silver_df = transform_to_silver(bronze_df)
    count = write_silver_incremental(spark, silver_df, silver_path)

    silver_df.createOrReplaceTempView("silver_transactions")
    logger.info(
        "silver_process_complete",
        records=count,
        path=silver_path,
    )
    return count
