"""End-to-end fraud detection ETL pipeline orchestrator."""

from __future__ import annotations

import argparse
import time
import uuid
from datetime import datetime

from fraud_detection.bronze.ingest import ingest_bronze
from fraud_detection.config import load_config
from fraud_detection.data_generation.generator import generate_transactions_from_config
from fraud_detection.gold.analytics import run_gold_analytics
from fraud_detection.gold.fraud_rules import score_transactions
from fraud_detection.logging_config import log_pipeline_event, setup_logging
from fraud_detection.postgres.export import export_gold_to_postgres, log_pipeline_run
from fraud_detection.silver.transform import process_silver
from fraud_detection.spark_session import create_spark_session


def run_pipeline(
    generate_data: bool = False,
    full_refresh: bool = False,
    skip_postgres: bool = False,
) -> dict:
    """Execute the full Bronze → Silver → Gold medallion pipeline."""
    config = load_config()
    logger = setup_logging(config.log_level)
    run_id = str(uuid.uuid4())
    start = time.time()

    log_pipeline_event(logger, "pipeline_started", "orchestrator", run_id=run_id)

    if generate_data:
        logger.info("generating_transaction_data", records=config.num_records)
        generate_transactions_from_config(config)

    spark = create_spark_session(config)

    try:
        bronze_count = ingest_bronze(spark, config, logger)
        silver_count = process_silver(spark, config, logger)
        scored_df = score_transactions(spark, config, logger)
        gold_stats = run_gold_analytics(spark, scored_df, config, logger)

        pg_stats = {}
        if not skip_postgres:
            pg_stats = export_gold_to_postgres(spark, config, logger)

        duration = time.time() - start
        stats = {
            "run_id": run_id,
            "bronze_records": bronze_count,
            "silver_records": silver_count,
            "gold_tables": gold_stats,
            "postgres_tables": pg_stats,
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS",
            "completed_at": datetime.utcnow().isoformat(),
        }

        if not skip_postgres:
            log_pipeline_run(spark, config, run_id, "SUCCESS", stats, duration)

        log_pipeline_event(
            logger,
            "pipeline_completed",
            "orchestrator",
            **stats,
        )
        return stats

    except Exception as exc:
        duration = time.time() - start
        logger.error("pipeline_failed", run_id=run_id, error=str(exc), duration=duration)
        raise
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Banking Fraud Detection Pipeline")
    parser.add_argument("--generate-data", action="store_true", help="Generate synthetic data")
    parser.add_argument("--full-refresh", action="store_true", help="Full pipeline refresh")
    parser.add_argument("--skip-postgres", action="store_true", help="Skip PostgreSQL export")
    args = parser.parse_args()

    stats = run_pipeline(
        generate_data=args.generate_data or args.full_refresh,
        full_refresh=args.full_refresh,
        skip_postgres=args.skip_postgres,
    )
    print(f"Pipeline completed: {stats}")


if __name__ == "__main__":
    main()
