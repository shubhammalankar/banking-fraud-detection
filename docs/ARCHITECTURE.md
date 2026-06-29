# Architecture Document

**Project:** Banking Fraud Detection Pipeline  
**Version:** 1.0.0  
**Owner:** Fraud Analytics Engineering  
**Classification:** Internal

---

## 1. Executive Summary

This document describes the architecture of the Banking Fraud Detection Pipeline — a production-grade batch analytics platform built on Apache Spark, Delta Lake, and PostgreSQL. The system ingests banking transaction events, applies a rules-based fraud scoring engine, and produces analytics for fraud operations teams.

## 2. System Context

```
┌──────────────────────────────────────────────────────────────────────┐
│                        External Systems                              │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────────┐ │
│  │ Transaction │   │  Airflow    │   │ Fraud Ops Dashboard         │ │
│  │ Source CSV  │   │  Scheduler  │   │ (PostgreSQL consumers)      │ │
│  └──────┬──────┘   └──────┬──────┘   └──────────────▲──────────────┘ │
└─────────┼─────────────────┼─────────────────────────┼────────────────┘
          │                 │                         │
          ▼                 ▼                         │
┌─────────────────────────────────────────────────────┴────────────────┐
│                    Fraud Detection Pipeline                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Bronze  │─▶│ Silver  │─▶│  Gold   │─▶│ Postgres │─▶│ Monitoring│ │
│  │ Ingest  │  │ Cleanse │  │ Analytics│  │  Export  │  │  & Audit  │ │
│  └─────────┘  └─────────┘  └─────────┘  └──────────┘  └───────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

## 3. Medallion Architecture

### 3.1 Bronze Layer

**Purpose:** Immutable raw ingestion with audit metadata.

| Attribute | Value |
|-----------|-------|
| Format | Delta Lake |
| Partition | `ingestion_date` |
| Write Mode | Incremental MERGE on `transaction_id` |
| Schema | String-typed raw fields + ingestion metadata |

**Key Operations:**
- CSV ingestion with schema-on-read
- Append ingestion timestamp and source file path
- Date-based repartitioning for write optimization

### 3.2 Silver Layer

**Purpose:** Trusted, cleansed transaction data conforming to enterprise schema.

| Attribute | Value |
|-----------|-------|
| Format | Delta Lake |
| Partition | `transaction_date` |
| Write Mode | Incremental MERGE (upsert) |
| Schema | Strongly typed with derived columns |

**Transformations:**
1. Type casting and validation
2. Null imputation with business defaults
3. Duplicate removal (latest ingestion wins)
4. Derived columns: `is_domestic`, `transaction_hour`, `transaction_date`

### 3.3 Gold Layer

**Purpose:** Fraud-scored transactions and aggregated analytics.

| Table | Description |
|-------|-------------|
| `fraud_scores` | Transaction-level fraud flags and scores |
| `fraud_rate` | Overall KPI |
| `fraud_by_country` | Geographic fraud distribution |
| `fraud_by_merchant` | Merchant risk analysis |
| `fraud_by_device` | Device-level fraud patterns |
| `daily_fraud_trend` | Time-series fraud metrics |
| `customer_risk_ranking` | Customer risk tiers (dense_rank) |
| `top_suspicious_accounts` | Top 50 flagged accounts |
| `rule_trigger_summary` | Rule effectiveness metrics |

## 4. Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Compute | Apache Spark (PySpark) | 3.5.1 |
| Storage | Delta Lake | 3.1.0 |
| Orchestration | Apache Airflow | 2.8.4 |
| Serving DB | PostgreSQL | 15 |
| Containerization | Docker Compose | 3.8 |
| Testing | pytest | 8.1.1 |
| Logging | structlog (JSON) | 24.1.0 |

## 5. Spark Optimization Strategy

### 5.1 Window Functions
All temporal fraud rules use Spark SQL window functions:
- `lag()` — previous transaction location, device, reset flag
- `lead()` — available for forward-looking patterns
- `dense_rank()` — customer and account risk rankings
- Range-based windows — velocity and burst detection

### 5.2 Broadcast Join
High-risk merchant category lookup table is broadcast to all executors, avoiding shuffle for the Rule 5 join.

### 5.3 Partitioning
- Bronze: partitioned by `ingestion_date`
- Silver/Gold: partitioned by `transaction_date`
- Repartition before write to optimize file sizes

### 5.4 Adaptive Query Execution (AQE)
Enabled globally:
- Coalesce shuffle partitions post-aggregation
- Skew join handling for high-volume customers
- Dynamic partition pruning on date filters

### 5.5 Explain Plans
Physical execution plans logged during Gold analytics for performance review and regression detection.

## 6. Incremental ETL Design

```
New CSV Batch
     │
     ▼
Bronze MERGE (whenNotMatchedInsert)
     │
     ▼
Silver MERGE (whenMatchedUpdate + whenNotMatchedInsert)
     │
     ▼
Gold OVERWRITE (full recompute on Silver snapshot)
     │
     ▼
PostgreSQL OVERWRITE (JDBC export)
```

Delta Lake MERGE operations ensure idempotent, exactly-once semantics at the Bronze and Silver layers.

## 7. Security Considerations

- Credentials managed via environment variables (never committed)
- PostgreSQL access restricted to pipeline service account
- No PII exposed in logs (transaction IDs only)
- `.env` excluded from version control

## 8. Deployment Topology

```
Docker Compose Stack:
├── postgres:5432        — Analytics serving layer
├── spark                — Batch pipeline runner
├── airflow-webserver:8080
└── airflow-scheduler    — DAG execution
```

## 9. SLAs

| Metric | Target |
|--------|--------|
| Pipeline completion | < 30 min for 50K records |
| Data freshness | Daily by 03:00 UTC |
| Fraud scoring latency | Batch (T+1) |
| Availability | 99.5% (batch) |
