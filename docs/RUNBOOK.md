# Operations Runbook

**Project:** Banking Fraud Detection Pipeline  
**On-Call:** Fraud Analytics Engineering  
**Escalation:** Data Platform Team

---

## 1. Daily Operations Checklist

- [ ] Verify Airflow DAG `fraud_detection_pipeline` completed successfully (by 03:30 UTC)
- [ ] Check fraud rate KPI is within expected range (2–15%)
- [ ] Review `pipeline_runs` audit table for failures
- [ ] Confirm PostgreSQL Gold tables refreshed

## 2. Starting the Stack

```bash
# Full stack (PostgreSQL + Spark + Airflow)
docker-compose up -d

# Pipeline only (local, no Docker)
pip install -r requirements.txt
export PYTHONPATH=src
bash scripts/run_pipeline.sh
```

## 3. Manual Pipeline Execution

```bash
# Generate data + full pipeline
python -m fraud_detection.etl.pipeline --full-refresh

# Pipeline only (existing data)
python -m fraud_detection.etl.pipeline

# Skip PostgreSQL export
python -m fraud_detection.etl.pipeline --skip-postgres
```

## 4. Monitoring

### 4.1 Structured Logs

Pipeline emits JSON logs with fields:
- `event`: pipeline_started | bronze_ingest_complete | pipeline_completed
- `layer`: bronze | silver | gold | orchestrator
- `records`: row count
- `duration_seconds`: elapsed time

### 4.2 Key Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Fraud rate | `gold/fraud_rate` | > 50% (anomaly) |
| Pipeline duration | logs | > 3600s |
| Bronze record count | logs | 0 (no data) |
| Failed tasks | Airflow UI | any failure |

### 4.3 Airflow UI

Access: http://localhost:8080

DAGs:
- `fraud_detection_pipeline` — Daily 02:00 UTC
- `fraud_detection_incremental` — Every 4 hours

## 5. Troubleshooting

### Pipeline fails at Bronze

**Symptom:** `bronze_ingest_skipped` or schema error

**Resolution:**
1. Check raw CSV exists: `data/raw/transactions/*.csv`
2. Regenerate: `python -m fraud_detection.data_generation.generator`
3. Verify CSV columns match Bronze schema

### Silver validation drops all records

**Symptom:** `silver_records: 0`

**Resolution:**
1. Inspect Bronze data for invalid statuses/payment types
2. Check amount fields are positive numbers
3. Review validation filters in `silver/transform.py`

### PostgreSQL export fails

**Symptom:** `postgres_export_failed` in logs

**Resolution:**
1. Verify PostgreSQL is running: `docker-compose ps postgres`
2. Check connectivity: `psql -h localhost -U fraud_user -d fraud_analytics`
3. Run with `--skip-postgres` to isolate Spark issues

### High fraud rate alert

**Symptom:** Airflow validation task fails (> 50% fraud rate)

**Resolution:**
1. Check data generator fraud injection rate
2. Review rule trigger summary: `gold/rule_trigger_summary`
3. Validate rule weights haven't changed unexpectedly

### Spark OOM

**Resolution:**
1. Increase driver memory in `spark_session.py`
2. Reduce `shuffle_partitions` in config.yaml
3. Enable AQE (already enabled by default)

## 6. Recovery Procedures

### Full Rebuild

```bash
rm -rf data/delta/*
python -m fraud_detection.etl.pipeline --full-refresh
```

### Replay Single Day

1. Delete Bronze partition for target date
2. Place CSV in raw path
3. Run incremental pipeline

## 7. Configuration Changes

All config changes go through `config/config.yaml`. Environment variables override YAML values.

After config change:
1. Run full pipeline in dev
2. Compare fraud rate before/after
3. Deploy via Docker rebuild

## 8. Contacts

| Role | Team |
|------|------|
| Pipeline Owner | Fraud Analytics Engineering |
| Infrastructure | Data Platform |
| Business Owner | Fraud Operations |
