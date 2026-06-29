"""Realistic banking transaction data generator with embedded fraud patterns."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from fraud_detection.config import AppConfig

fake = Faker()
Faker.seed(42)
random.seed(42)

MERCHANT_CATEGORIES = [
    "groceries",
    "restaurants",
    "retail",
    "travel",
    "utilities",
    "healthcare",
    "entertainment",
    "transportation",
    "gambling",
    "cryptocurrency",
    "money_transfer",
    "adult_entertainment",
    "electronics",
    "fuel",
    "insurance",
]

PAYMENT_TYPES = ["credit_card", "debit_card", "wire_transfer", "ach", "mobile_pay"]

CITY_COORDS = {
    "New York": {"country": "US", "lat": 40.7128, "lon": -74.0060},
    "London": {"country": "GB", "lat": 51.5074, "lon": -0.1278},
    "Paris": {"country": "FR", "lat": 48.8566, "lon": 2.3522},
    "Tokyo": {"country": "JP", "lat": 35.6762, "lon": 139.6503},
    "Singapore": {"country": "SG", "lat": 1.3521, "lon": 103.8198},
    "Sydney": {"country": "AU", "lat": -33.8688, "lon": 151.2093},
    "Toronto": {"country": "CA", "lat": 43.6532, "lon": -79.3832},
    "Chicago": {"country": "US", "lat": 41.8781, "lon": -87.6298},
    "Los Angeles": {"country": "US", "lat": 34.0522, "lon": -118.2437},
    "Mumbai": {"country": "IN", "lat": 19.0760, "lon": 72.8777},
    "Dubai": {"country": "AE", "lat": 25.2048, "lon": 55.2708},
    "Frankfurt": {"country": "DE", "lat": 50.1109, "lon": 8.6821},
}

DOMESTIC_COUNTRY = "US"


def _generate_base_transaction(
    customer_id: str,
    account_id: str,
    device_id: str,
    session_id: str,
    base_time: datetime,
    city: str,
    amount: float | None = None,
    category: str | None = None,
    status: str = "approved",
    password_reset: bool = False,
) -> dict:
    coords = CITY_COORDS[city]
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": customer_id,
        "timestamp": base_time.isoformat(),
        "amount": round(amount or random.uniform(10, 500), 2),
        "merchant": fake.company(),
        "merchant_category": category or random.choice(MERCHANT_CATEGORIES[:8]),
        "city": city,
        "country": coords["country"],
        "latitude": coords["lat"] + random.uniform(-0.05, 0.05),
        "longitude": coords["lon"] + random.uniform(-0.05, 0.05),
        "payment_type": random.choice(PAYMENT_TYPES),
        "device_id": device_id,
        "ip_address": fake.ipv4(),
        "account_balance": round(random.uniform(1000, 50000), 2),
        "transaction_status": status,
        "password_reset_flag": password_reset,
        "session_id": session_id,
    }


def _inject_fraud_patterns(
    records: list[dict],
    customer_profiles: dict[str, dict],
) -> None:
    """Inject known fraud patterns for rule validation."""
    fraud_customers = list(customer_profiles.keys())[:400]

    # Rule 1: Amount exceeds 5x customer average
    for cid in fraud_customers[:50]:
        profile = customer_profiles[cid]
        avg = profile["avg_amount"]
        records.append(
            _generate_base_transaction(
                cid,
                profile["account_id"],
                profile["device_id"],
                profile["session_id"],
                profile["base_time"] + timedelta(hours=random.randint(1, 48)),
                random.choice(list(CITY_COORDS.keys())),
                amount=avg * random.uniform(6, 15),
            )
        )

    # Rule 2: >5 transactions in 2 minutes
    for cid in fraud_customers[50:80]:
        profile = customer_profiles[cid]
        burst_time = profile["base_time"] + timedelta(days=3)
        for i in range(7):
            records.append(
                _generate_base_transaction(
                    cid,
                    profile["account_id"],
                    profile["device_id"],
                    profile["session_id"],
                    burst_time + timedelta(seconds=i * 15),
                    "Chicago",
                    amount=random.uniform(20, 100),
                )
            )

    # Rule 3: Impossible travel NY -> London in 30 min
    for cid in fraud_customers[80:110]:
        profile = customer_profiles[cid]
        t0 = profile["base_time"] + timedelta(days=5)
        records.append(
            _generate_base_transaction(cid, profile["account_id"], profile["device_id"], profile["session_id"], t0, "New York")
        )
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                t0 + timedelta(minutes=20), "London", amount=random.uniform(500, 2000),
            )
        )

    # Rule 4: International immediately after domestic
    for cid in fraud_customers[110:140]:
        profile = customer_profiles[cid]
        t0 = profile["base_time"] + timedelta(days=6)
        records.append(
            _generate_base_transaction(cid, profile["account_id"], profile["device_id"], profile["session_id"], t0, "Los Angeles")
        )
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                t0 + timedelta(minutes=2), "Tokyo", amount=random.uniform(300, 1500),
            )
        )

    # Rule 5: High-risk merchant category
    for cid in fraud_customers[140:180]:
        profile = customer_profiles[cid]
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                profile["base_time"] + timedelta(days=7),
                random.choice(list(CITY_COORDS.keys())),
                category=random.choice(["gambling", "cryptocurrency", "money_transfer", "adult_entertainment"]),
                amount=random.uniform(500, 5000),
            )
        )

    # Rule 6: Multiple declined transactions
    for cid in fraud_customers[180:210]:
        profile = customer_profiles[cid]
        t0 = profile["base_time"] + timedelta(days=8)
        for i in range(4):
            records.append(
                _generate_base_transaction(
                    cid, profile["account_id"], profile["device_id"], profile["session_id"],
                    t0 + timedelta(minutes=i * 5), "Chicago", status="declined",
                )
            )

    # Rule 7: Night-time unusual spending
    for cid in fraud_customers[210:240]:
        profile = customer_profiles[cid]
        night_time = profile["base_time"].replace(hour=2, minute=30) + timedelta(days=9)
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                night_time, "New York", amount=random.uniform(2000, 8000),
            )
        )

    # Rule 8: Device changed within same session
    for cid in fraud_customers[240:270]:
        profile = customer_profiles[cid]
        session = str(uuid.uuid4())
        t0 = profile["base_time"] + timedelta(days=10)
        records.append(
            _generate_base_transaction(cid, profile["account_id"], profile["device_id"], session, t0, "Chicago")
        )
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], str(uuid.uuid4()), session,
                t0 + timedelta(minutes=3), "Chicago", amount=random.uniform(100, 500),
            )
        )

    # Rule 9: Large withdrawal after password reset
    for cid in fraud_customers[270:300]:
        profile = customer_profiles[cid]
        t0 = profile["base_time"] + timedelta(days=11)
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                t0, "New York", password_reset=True, amount=100,
            )
        )
        records.append(
            _generate_base_transaction(
                cid, profile["account_id"], profile["device_id"], profile["session_id"],
                t0 + timedelta(minutes=30), "New York",
                amount=random.uniform(6000, 15000), category="money_transfer",
            )
        )

    # Rule 10: Velocity fraud (24h high volume)
    for cid in fraud_customers[300:330]:
        profile = customer_profiles[cid]
        t0 = profile["base_time"] + timedelta(days=12)
        for i in range(25):
            records.append(
                _generate_base_transaction(
                    cid, profile["account_id"], profile["device_id"], profile["session_id"],
                    t0 + timedelta(hours=i * 0.5), random.choice(list(CITY_COORDS.keys())),
                    amount=random.uniform(400, 800),
                )
            )


def generate_transactions(
    num_records: int = 50000,
    num_customers: int = 5000,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Generate realistic banking transactions with embedded fraud patterns."""
    records: list[dict] = []
    customer_profiles: dict[str, dict] = {}

    start_date = datetime(2024, 1, 1)

    for i in range(num_customers):
        cid = f"CUST-{i:06d}"
        customer_profiles[cid] = {
            "account_id": f"ACC-{i:06d}",
            "device_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "base_time": start_date + timedelta(days=random.randint(0, 60)),
            "avg_amount": random.uniform(50, 300),
            "home_city": random.choice(list(CITY_COORDS.keys())),
        }

    base_count = num_records
    for _ in range(base_count):
        cid = random.choice(list(customer_profiles.keys()))
        profile = customer_profiles[cid]
        tx_time = profile["base_time"] + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
        )
        records.append(
            _generate_base_transaction(
                cid,
                profile["account_id"],
                profile["device_id"],
                profile["session_id"],
                tx_time,
                random.choice([profile["home_city"]] * 3 + list(CITY_COORDS.keys())),
                amount=random.lognormvariate(4, 1),
            )
        )

    _inject_fraud_patterns(records, customer_profiles)

    df = pd.DataFrame(records)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    df = df.head(num_records)

    if output_path:
        path = Path(output_path)
        path.mkdir(parents=True, exist_ok=True)
        partition_date = datetime.utcnow().strftime("%Y-%m-%d")
        out_file = path / f"transactions_{partition_date}.csv"
        df.to_csv(out_file, index=False)

    return df


def generate_transactions_from_config(config: AppConfig) -> pd.DataFrame:
    """Generate transactions using application configuration."""
    return generate_transactions(
        num_records=config.num_records,
        output_path=config.paths.raw_data,
    )


if __name__ == "__main__":
    from fraud_detection.config import load_config

    cfg = load_config()
    df = generate_transactions_from_config(cfg)
    print(f"Generated {len(df)} transactions -> {cfg.paths.raw_data}")
