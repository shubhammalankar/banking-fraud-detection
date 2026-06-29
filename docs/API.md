# PostgreSQL Serving Layer Schema

**Database:** `fraud_analytics`  
**Access:** Read-only for dashboards; write for pipeline service account

---

## Gold Analytics Tables

### fraud_scores

Transaction-level fraud scoring results.

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | VARCHAR | Primary key |
| customer_id | VARCHAR | Customer identifier |
| account_id | VARCHAR | Account identifier |
| timestamp | TIMESTAMP | Transaction time |
| amount | DOUBLE | Transaction amount |
| fraud_score | INTEGER | Weighted rule was score (0–165) |
| is_fraudulent | BOOLEAN | Score ≥ 25 |
| triggered_rules | VARCHAR | Comma-separated rule IDs |
| rule_1..rule_10 | BOOLEAN | Individual rule flags |

### fraud_rate

| Column | Type | Description |
|--------|------|-------------|
| total_transactions | BIGINT | Total count |
| fraudulent_transactions | BIGINT | Fraud count |
| fraud_rate_pct | DOUBLE | Fraud percentage |

### fraud_by_country

| Column | Type | Description |
|--------|------|-------------|
| country | VARCHAR | ISO country code |
| total_transactions | BIGINT | Count |
| fraud_count | BIGINT | Fraud count |
| fraud_rate_pct | DOUBLE | Rate |
| fraud_amount | DOUBLE | Total fraud exposure |

### fraud_by_merchant

Top 100 merchants by fraud count.

### fraud_by_device

Top 100 devices by fraud count.

### daily_fraud_trend

Daily time-series partitioned by `transaction_date`.

### customer_risk_ranking

| Column | Type | Description |
|--------|------|-------------|
| customer_id | VARCHAR | Customer ID |
| risk_rank | INTEGER | dense_rank by fraud score |
| risk_tier | VARCHAR | CRITICAL / HIGH / MEDIUM / LOW |
| fraud_exposure | DOUBLE | Total fraud amount |

### top_suspicious_accounts

Top 50 accounts with `suspicion_level`: SEVERE / HIGH / ELEVATED / MODERATE.

### rule_trigger_summary

| Column | Type | Description |
|--------|------|-------------|
| rule_name | VARCHAR | Rule column name |
| trigger_count | BIGINT | Times rule fired |

### pipeline_runs (Audit)

| Column | Type | Description |
|--------|------|-------------|
| run_id | VARCHAR | UUID |
| status | VARCHAR | SUCCESS / FAILED |
| layer_stats | TEXT | JSON stats |
| duration_seconds | DOUBLE | Runtime |
| created_at | TIMESTAMP | Auto-generated |

---

## Sample Queries

```sql
-- Daily fraud rate trend
SELECT transaction_date, fraud_rate_pct, fraud_count
FROM daily_fraud_trend
ORDER BY transaction_date DESC
LIMIT 30;

-- Top critical customers
SELECT customer_id, risk_rank, risk_tier, fraud_exposure
FROM customer_risk_ranking
WHERE risk_tier = 'CRITICAL';

-- Rule effectiveness
SELECT rule_name, trigger_count
FROM rule_trigger_summary
ORDER BY trigger_count DESC;
```
