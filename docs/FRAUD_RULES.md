# Fraud Detection Rules Specification

**Document ID:** FRAUD-RULES-001  
**Version:** 1.0.0  
**Effective Date:** 2024-01-01  
**Owner:** Fraud Analytics Engineering

---

## Overview

The fraud detection engine applies 10 rules to each transaction, computing a weighted fraud score. Transactions scoring ≥ 25 are flagged as fraudulent.

## Scoring Model

| Score Range | Classification |
|-------------|---------------|
| 0–24 | Normal |
| 25–39 | Elevated Risk |
| 40–59 | High Risk |
| 60+ | Severe / Block |

---

## Rule Definitions

### Rule 1: Amount Anomaly (Weight: 15)

**Description:** Transaction amount exceeds the customer's historical average by 5× or more.

**Logic:**
```sql
amount > AVG(amount) OVER (PARTITION BY customer_id) * 5
```

**Rationale:** Sudden large transactions relative to spending history indicate account takeover or card cloning.

**False Positive Mitigation:** Requires sufficient transaction history; new customers excluded by low average.

---

### Rule 2: Velocity Burst (Weight: 20)

**Description:** More than 5 transactions within a 2-minute sliding window.

**Logic:**
```sql
COUNT(*) OVER (
  PARTITION BY customer_id
  ORDER BY timestamp
  RANGE BETWEEN CURRENT ROW AND 120 FOLLOWING
) > 5
```

**Rationale:** Rapid-fire transactions suggest automated fraud bots or card testing.

---

### Rule 3: Impossible Travel (Weight: 25)

**Description:** Transaction in a distant city within 30 minutes of a previous transaction in another city.

**City Pairs Monitored:**
- New York ↔ London
- Los Angeles ↔ Tokyo
- Chicago ↔ Dubai

**Logic:**
```sql
LAG(city) != city
AND (timestamp - LAG(timestamp)) <= 30 minutes
AND city_pair IN impossible_pairs
```

**Rationale:** Physically impossible travel indicates card data compromise across geographies.

---

### Rule 4: International After Domestic (Weight: 15)

**Description:** International transaction within 5 minutes of a domestic transaction.

**Logic:**
```sql
LAG(is_domestic) = TRUE
AND is_domestic = FALSE
AND (timestamp - LAG(timestamp)) <= 5 minutes
```

---

### Rule 5: High-Risk Merchant (Weight: 10)

**Description:** Transaction at a high-risk merchant category.

**Categories:** gambling, cryptocurrency, money_transfer, adult_entertainment

**Implementation:** Broadcast join against merchant category lookup table.

---

### Rule 6: Multiple Declined (Weight: 15)

**Description:** 3 or more declined transactions within 1 hour.

**Logic:**
```sql
SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END)
  OVER (PARTITION BY customer_id ORDER BY timestamp RANGE -3600 TO 0) >= 3
```

---

### Rule 7: Night-Time Spending (Weight: 10)

**Description:** Transaction between 00:00–05:00 exceeding 3× customer average amount.

**Rationale:** Unusual spending during sleep hours may indicate unauthorized access.

---

### Rule 8: Device Change (Weight: 15)

**Description:** Different device ID within the same session.

**Logic:**
```sql
LAG(device_id) OVER (PARTITION BY customer_id, session_id ORDER BY timestamp) != device_id
```

---

### Rule 9: Post-Reset Withdrawal (Weight: 20)

**Description:** Withdrawal ≥ $5,000 within 2 hours of a password reset event.

**Rationale:** Credential compromise followed by fund extraction.

---

### Rule 10: Velocity Fraud (Weight: 20)

**Description:** 24-hour rolling window with ≥ 20 transactions AND total amount ≥ $10,000.

**Rationale:** Structuring or mule account activity pattern.

---

## Rule Combination Examples

| Scenario | Rules Triggered | Score | Action |
|----------|----------------|-------|--------|
| Card testing + impossible travel | R2, R3 | 45 | Block + Review |
| Account takeover | R8, R9 | 35 | Block + Alert |
| High-risk merchant only | R5 | 10 | Monitor |
| Full compromise pattern | R1,R3,R8,R9,R10 | 95 | Block + SAR |

---

## Change Management

Rule weight changes require:
1. Fraud Analytics sign-off
2. Backtesting on 90-day historical data
3. A/B validation period (7 days shadow mode)
4. Update to this document and config.yaml
