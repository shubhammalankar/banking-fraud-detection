"""Configuration loader for the fraud detection pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


@dataclass
class SparkConfig:
    app_name: str = "fraud-detection-pipeline"
    master: str = "local[*]"
    adaptive_query_execution: bool = True
    shuffle_partitions: int = 200
    broadcast_threshold: int = 10485760


@dataclass
class PathsConfig:
    raw_data: str = "data/raw/transactions"
    bronze: str = "data/delta/bronze/transactions"
    silver: str = "data/delta/silver/transactions"
    gold: str = "data/delta/gold"
    checkpoints: str = "data/checkpoints"


@dataclass
class FraudRulesConfig:
    amount_multiplier_threshold: float = 5.0
    velocity_window_minutes: int = 2
    velocity_transaction_count: int = 5
    impossible_travel_minutes: int = 30
    high_risk_categories: list[str] = field(
        default_factory=lambda: [
            "gambling",
            "cryptocurrency",
            "money_transfer",
            "adult_entertainment",
        ]
    )
    declined_threshold: int = 3
    night_start_hour: int = 0
    night_end_hour: int = 5
    large_withdrawal_threshold: float = 5000.0
    velocity_24h_amount_threshold: float = 10000.0
    velocity_24h_count_threshold: int = 20


@dataclass
class PostgresConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "fraud_analytics"
    user: str = "fraud_user"
    password: str = "fraud_pass"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass
class AppConfig:
    spark: SparkConfig = field(default_factory=SparkConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    fraud_rules: FraudRulesConfig = field(default_factory=FraudRulesConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    log_level: str = "INFO"
    environment: str = "development"
    num_records: int = 50000


def _resolve_path(path: str) -> str:
    p = Path(path)
    if p.is_absolute():
        return str(p)
    return str(PROJECT_ROOT / p)


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load configuration from YAML with environment variable overrides."""
    path = config_path or DEFAULT_CONFIG_PATH
    raw: dict[str, Any] = {}
    if path.exists():
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    spark_raw = raw.get("spark", {})
    paths_raw = raw.get("paths", {})
    fraud_raw = raw.get("fraud_rules", {})
    pg_raw = raw.get("postgres", {})
    gen_raw = raw.get("data_generation", {})
    logging_raw = raw.get("logging", {})

    paths = PathsConfig(
        raw_data=_resolve_path(os.getenv("RAW_DATA_PATH", paths_raw.get("raw_data", "data/raw/transactions"))),
        bronze=_resolve_path(os.getenv("DELTA_BRONZE_PATH", paths_raw.get("bronze", "data/delta/bronze/transactions"))),
        silver=_resolve_path(os.getenv("DELTA_SILVER_PATH", paths_raw.get("silver", "data/delta/silver/transactions"))),
        gold=_resolve_path(os.getenv("DELTA_GOLD_PATH", paths_raw.get("gold", "data/delta/gold"))),
        checkpoints=_resolve_path(paths_raw.get("checkpoints", "data/checkpoints")),
    )

    return AppConfig(
        spark=SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", spark_raw.get("app_name", "fraud-detection-pipeline")),
            master=os.getenv("SPARK_MASTER", spark_raw.get("master", "local[*]")),
            adaptive_query_execution=spark_raw.get("adaptive_query_execution", True),
            shuffle_partitions=spark_raw.get("shuffle_partitions", 200),
            broadcast_threshold=spark_raw.get("broadcast_threshold", 10485760),
        ),
        paths=paths,
        fraud_rules=FraudRulesConfig(
            amount_multiplier_threshold=fraud_raw.get("amount_multiplier_threshold", 5.0),
            velocity_window_minutes=fraud_raw.get("velocity_window_minutes", 2),
            velocity_transaction_count=fraud_raw.get("velocity_transaction_count", 5),
            impossible_travel_minutes=fraud_raw.get("impossible_travel_minutes", 30),
            high_risk_categories=fraud_raw.get("high_risk_categories", FraudRulesConfig().high_risk_categories),
            declined_threshold=fraud_raw.get("declined_threshold", 3),
            night_start_hour=fraud_raw.get("night_start_hour", 0),
            night_end_hour=fraud_raw.get("night_end_hour", 5),
            large_withdrawal_threshold=fraud_raw.get("large_withdrawal_threshold", 5000.0),
            velocity_24h_amount_threshold=fraud_raw.get("velocity_24h_amount_threshold", 10000.0),
            velocity_24h_count_threshold=fraud_raw.get("velocity_24h_count_threshold", 20),
        ),
        postgres=PostgresConfig(
            host=os.getenv("POSTGRES_HOST", pg_raw.get("host", "localhost")),
            port=int(os.getenv("POSTGRES_PORT", pg_raw.get("port", 5432))),
            database=os.getenv("POSTGRES_DB", pg_raw.get("database", "fraud_analytics")),
            user=os.getenv("POSTGRES_USER", pg_raw.get("user", "fraud_user")),
            password=os.getenv("POSTGRES_PASSWORD", pg_raw.get("password", "fraud_pass")),
        ),
        log_level=os.getenv("LOG_LEVEL", logging_raw.get("level", "INFO")),
        environment=os.getenv("ENVIRONMENT", "development"),
        num_records=gen_raw.get("num_records", 50000),
    )
