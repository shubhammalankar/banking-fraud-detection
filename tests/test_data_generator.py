"""Tests for transaction data generator."""

from fraud_detection.data_generation.generator import generate_transactions

REQUIRED_COLUMNS = [
    "transaction_id",
    "account_id",
    "customer_id",
    "timestamp",
    "amount",
    "merchant",
    "merchant_category",
    "city",
    "country",
    "latitude",
    "longitude",
    "payment_type",
    "device_id",
    "ip_address",
    "account_balance",
    "transaction_status",
    "password_reset_flag",
    "session_id",
]


def test_generate_transactions_count():
    df = generate_transactions(num_records=1000, num_customers=100)
    assert len(df) == 1000


def test_generate_transactions_schema():
    df = generate_transactions(num_records=500, num_customers=50)
    assert list(df.columns) == REQUIRED_COLUMNS


def test_generate_transactions_no_null_ids():
    df = generate_transactions(num_records=500, num_customers=50)
    assert df["transaction_id"].notna().all()
    assert df["customer_id"].notna().all()
    assert df["account_id"].notna().all()


def test_generate_transactions_positive_amounts():
    df = generate_transactions(num_records=500, num_customers=50)
    assert (df["amount"] > 0).all()


def test_generate_transactions_valid_statuses():
    df = generate_transactions(num_records=500, num_customers=50)
    valid = {"approved", "declined", "pending", "reversed"}
    assert set(df["transaction_status"].unique()).issubset(valid)
