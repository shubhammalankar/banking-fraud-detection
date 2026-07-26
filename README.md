# Banking Fraud Detection Pipeline

A production-style, PySpark-based fraud detection pipeline built around the **Medallion Architecture** (Bronze → Silver → Gold). It ingests raw banking transactions, cleanses and validates them, scores them against a 10-rule fraud engine, computes analytics KPIs, and serves the results to PostgreSQL — all orchestrated with Apache Airflow and packaged for Docker.

> **Note:** This README was regenerated from the project source. The previous `README.md` in this repository had been accidentally overwritten with the contents of `src/fraud_detection/gold/analytics.py` (with only the final section of the original README surviving). Nothing in the source code was changed.

---

## Overview

| | |
|---|---|
| **Domain** | Banking / financial transaction fraud detection |
| **Engine** | PySpark 3.5 + Delta Lake 3.1 |
| **Architecture** | Medallion (Bronze / Silver / Gold) |
| **Orchestration** | Apache Airflow |
| **Serving layer** | PostgreSQL |
| **Packaging** | Docker / Docker Compose |
| **Testing** | pytest |

The pipeline generates (or ingests) transaction-level data, runs it through a validated ETL flow, flags suspicious transactions using 10 independent fraud rules, and produces a set of analytics tables ready for dashboards or downstream consumption.

---

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Raw CSV    │───▶│   Bronze    │───▶│   Silver    │───▶│    Gold     │
│  (Source)   │    │  (Ingest)   │    │ (Cleanse)   │    │ (Analytics) │
└─────────────┘    └─────────────┘    └─────────────┘    └──────┬──────┘
                                                                 │
                                                                 ▼
                                                          ┌─────────────┐
                                                          │ PostgreSQL  │
                                                          │ (Serving)   │
                                                          └─────────────┘
```

### Medallion layers

| Layer | Purpose | Storage |
|-------|---------|---------|
| **Bronze** | Raw CSV ingestion with metadata enrichment (`ingestion_timestamp`, `source_file`, `ingestion_date`); idempotent `MERGE` upserts by `transaction_id` | Delta Lake, partitioned by `ingestion_date` |
| **Silver** | Type casting, validation (nulls, valid statuses/payment types), null handling/imputation, deduplication, schema enforcement | Delta Lake, partitioned by `transaction_date` |
| **Gold** | 10-rule fraud scoring engine + KPI/analytics tables (fraud rate, by-country, by-merchant, by-device, daily trend, customer risk ranking, top suspicious accounts, rule trigger summary) | Delta Lake + PostgreSQL |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full system design and data flow.

---

## Fraud detection rules

Each transaction is evaluated against 10 independent rules. Triggered rules contribute their weight to a cumulative `fraud_score`; a transaction is flagged `is_fraudulent` once the score crosses the threshold.

| Rule | Description | Weight |
|------|-------------|--------|
| `rule_1_amount_anomaly` | Amount exceeds 5× the customer's average transaction amount | 15 |
| `rule_2_velocity_burst` | More than 5 transactions within a 2-minute window | 20 |
| `rule_3_impossible_travel` | Geographically impossible travel between consecutive transactions (e.g. New York → London within 30 minutes) | 25 |
| `rule_4_intl_after_domestic` | International transaction immediately following a domestic one | 15 |
| `rule_5_high_risk_merchant` | Merchant category in the high-risk list (gambling, cryptocurrency, money transfer, adult entertainment) | 10 |
| `rule_6_multiple_declined` | 3 or more declined transactions within a 1-hour window | 15 |
| `rule_7_night_spending` | Unusual spending during night hours (00:00–05:00) | 10 |
| `rule_8_device_change` | Device changes within the same session | 15 |
| `rule_9_post_reset_withdrawal` | Large withdrawal shortly after a password reset | 20 |
| `rule_10_velocity_fraud` | 24-hour velocity fraud (combined amount + transaction count thresholds) | 20 |

**Fraud threshold:** `fraud_score >= 25` → flagged as fraudulent (`FRAUD_SCORE_THRESHOLD` in `gold/fraud_rules.py`).

Rule logic is implemented with Spark window functions (`lag`, range-based windows, `dense_rank`) in [`src/fraud_detection/gold/fraud_rules.py`](src/fraud_detection/gold/fraud_rules.py). Full specifications live in [`docs/FRAUD_RULES.md`](docs/FRAUD_RULES.md).

---

## Project structure

```
banking-fraud-detection/
├── config/
│   └── config.yaml                 # Spark, paths, fraud-rule thresholds, Postgres, logging
├── dags/
│   └── fraud_detection_dag.py      # Airflow DAGs (full + incremental)
├── docker/
│   ├── Dockerfile                  # Spark pipeline image
│   ├── Dockerfile.airflow          # Airflow image
│   └── init-db.sql                 # PostgreSQL bootstrap schema
├── docs/
│   ├── ARCHITECTURE.md             # System design & data flow
│   ├── FRAUD_RULES.md              # Fraud rule specifications
│   ├── RUNBOOK.md                  # Operational runbook
│   └── API.md                      # PostgreSQL serving-layer schema
├── scripts/
│   ├── run_pipeline.sh             # Linux/macOS pipeline runner
│   └── run_pipeline.bat            # Windows pipeline runner
├── src/fraud_detection/
│   ├── bronze/ingest.py            # Raw ingestion → Delta (Bronze)
│   ├── silver/transform.py         # Validation, cleansing, dedup → Delta (Silver)
│   ├── gold/fraud_rules.py         # 10-rule fraud scoring engine
│   ├── gold/analytics.py           # KPI & analytics table builders
│   ├── postgres/export.py          # JDBC export of Gold tables to PostgreSQL
│   ├── data_generation/generator.py# Synthetic transaction data generator (Faker)
│   ├── etl/pipeline.py             # End-to-end orchestrator (CLI entry point)
│   ├── config.py                   # YAML + env-var configuration loader
│   ├── schemas.py                  # Bronze/Silver/Gold Spark schemas
│   ├── spark_session.py            # SparkSession factory (Delta + AQE config)
│   └── logging_config.py           # Structured JSON logging (structlog)
├── tests/                          # pytest suite
├── docker-compose.yml              # Postgres + Spark + Airflow stack
├── pyproject.toml
└── requirements.txt
```

---

## Getting started

### 1. Prerequisites

- Python 3.10+
- Java 8/11/17 (required by PySpark)
- Docker & Docker Compose (optional, for the full Postgres/Airflow stack)

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# or, for an editable install with dev/test extras:
pip install -e ".[dev]"
```

### 3. Configure environment

Copy the example environment file and adjust as needed:

```bash
cp .env.example .env
```

Key settings (see [`config/config.yaml`](config/config.yaml) and [`.env.example`](.env.example)):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fraud_analytics
POSTGRES_USER=fraud_user
POSTGRES_PASSWORD=fraud_pass
```

### 4. Generate data and run the pipeline

```bash
python -m fraud_detection.data_generation.generator
python -m fraud_detection.etl.pipeline --full-refresh
```

Or use the convenience script, which generates data and runs the pipeline without a PostgreSQL export:

```bash
./scripts/run_pipeline.sh          # Linux/macOS
scripts\run_pipeline.bat           # Windows
```

CLI flags for `fraud_detection.etl.pipeline`:

| Flag | Effect |
|------|--------|
| `--generate-data` | Generate synthetic transactions before running the pipeline |
| `--full-refresh` | Regenerate data and run the full Bronze→Silver→Gold pipeline |
| `--skip-postgres` | Skip the PostgreSQL export step |

### 5. Run with Docker Compose (Postgres + Spark + Airflow)

```bash
docker compose up -d
```

This starts:
- `postgres` — serving-layer database (with schema from `docker/init-db.sql`)
- `spark` — one-shot container that generates data and runs the full pipeline
- `airflow-init` / `airflow-webserver` / `airflow-scheduler` — Airflow stack for scheduled runs (webserver on `http://localhost:8080`)

### 6. Run tests

```bash
pytest tests/ -v --tb=short
```

Test coverage includes:
- Synthetic data generator output validation
- Silver-layer transformation (null handling, deduplication, validation rules)
- All 10 fraud rules against synthetic edge cases, plus overall fraud score computation

---

## Configuration

Configuration is layered: [`config/config.yaml`](config/config.yaml) provides defaults, and environment variables (loaded via `.env`) override them at runtime. This is handled by [`src/fraud_detection/config.py`](src/fraud_detection/config.py), which exposes a typed `AppConfig` dataclass covering:

- **Spark** — app name, master, adaptive query execution, shuffle partitions, broadcast threshold
- **Paths** — raw data, Bronze/Silver/Gold Delta locations, checkpoints
- **Data generation** — number of records/customers/accounts, fraud injection rate
- **Fraud rules** — thresholds for every rule (amount multiplier, velocity windows, travel time, high-risk categories, declined-transaction threshold, night-hours window, withdrawal thresholds)
- **PostgreSQL** — host, port, database, credentials, JDBC URL
- **Logging** — level and format

---

## Spark optimizations

- **Window functions** — `lag`, `dense_rank`, and range-based windows for velocity/travel rules and risk ranking
- **Broadcast joins** — for the high-risk merchant lookup
- **Partitioning** — date-based partitioning across Bronze (`ingestion_date`), Silver, and Gold layers
- **Adaptive Query Execution (AQE)** — enabled for skew handling and partition coalescing
- **Incremental ETL** — Delta Lake `MERGE` for idempotent Bronze/Silver loads
- **Explain plans** — logged during Gold analytics for query review

---

## Orchestration (Airflow)

Defined in [`dags/fraud_detection_dag.py`](dags/fraud_detection_dag.py):

| DAG | Schedule | Description |
|-----|----------|--------------|
| `fraud_detection_pipeline` | `0 2 * * *` (daily, 02:00 UTC) | Full medallion pipeline |
| `fraud_detection_incremental` | `0 */4 * * *` (every 4 hours) | Incremental Bronze → Silver → Gold refresh |

Both DAGs retry twice with a 5-minute delay and a 2-hour execution timeout, and include a post-run validation hook.

---

## Serving layer

Gold analytics tables are exported to PostgreSQL via JDBC (`postgres/export.py`):

`fraud_scores`, `fraud_rate`, `fraud_by_country`, `fraud_by_merchant`, `fraud_by_device`, `daily_fraud_trend`, `customer_risk_ranking`, `top_suspicious_accounts`, `rule_trigger_summary` — plus a `pipeline_runs` audit table logging each run's status and stats.

See [`docs/API.md`](docs/API.md) for the full serving-layer schema.

---

## Monitoring & logging

- Structured JSON logging via `structlog` ([`logging_config.py`](src/fraud_detection/logging_config.py))
- Pipeline events tagged with `layer`, `records`, and `duration_seconds`
- Airflow task-level monitoring
- `pipeline_runs` audit table in PostgreSQL

---

## Documentation

| Document | Description |
|----------|-------------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | System design and data flow |
| [`docs/FRAUD_RULES.md`](docs/FRAUD_RULES.md) | Fraud rule specifications |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | Operations runbook |
| [`docs/API.md`](docs/API.md) | PostgreSQL serving-layer schema |

---

## License

Internal use — Global Investment Bank Fraud Analytics Platform.
