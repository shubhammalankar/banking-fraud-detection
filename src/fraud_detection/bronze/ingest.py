"""Bronze layer: raw transaction ingestion into Delta Lake."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fraud_detection.config import AppConfig
from fraud_detection.schemas import BRONZE_TRANSACTION_SCHEMA


def read_raw_csv(spark: SparkSession, raw_path: str) -> DataFrame:
    """Read raw CSV files with schema-on-read enforcement."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(BRONZE_TRANSACTION_SCHEMA)
        .csv(f"{raw_path}/*.csv")
    )


def enrich_bronze(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns."""
    return (
        df.withColumn("ingestion_timestamp", F.lit(datetime.utcnow().isoformat()))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_date", F.to_date(F.col("ingestion_timestamp")))
    )


def write_bronze_incremental(
    spark: SparkSession,
    df: DataFrame,
    bronze_path: str,
    merge_key: str = "transaction_id",
) -> int:
    """
    Incrementally merge new transactions into Bronze Delta table.
    Uses MERGE for idempotent ingestion.
    """
    enriched = enrich_bronze(df)
    enriched = enriched.repartition(F.col("ingestion_date"))

    if DeltaTable.isDeltaTable(spark, bronze_path):
        delta_table = DeltaTable.forPath(spark, bronze_path)
        (
            delta_table.alias("target")
            .merge(
                enriched.alias("source"),
                f"target.{merge_key} = source.{merge_key}",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        Path(bronze_path).parent.mkdir(parents=True, exist_ok=True)
        (
            enriched.write.format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(bronze_path)
        )

    return enriched.count()


def ingest_bronze(spark: SparkSession, config: AppConfig, logger) -> int:
    """Execute Bronze layer ingestion pipeline."""
    raw_path = config.paths.raw_data
    bronze_path = config.paths.bronze

    if not list(Path(raw_path).glob("*.csv")):
        logger.warning("bronze_ingest_skipped", reason="no_raw_files", path=raw_path)
        return 0

    df = read_raw_csv(spark, raw_path)
    count = write_bronze_incremental(spark, df, bronze_path)
    logger.info("bronze_ingest_complete", records=count, path=bronze_path)
    return count
