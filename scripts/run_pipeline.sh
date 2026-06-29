#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"

cd "$PROJECT_ROOT"

echo "=== Banking Fraud Detection Pipeline ==="
echo "Step 1: Generate transaction data"
python -m fraud_detection.data_generation.generator

echo "Step 2: Run medallion ETL pipeline"
python -m fraud_detection.etl.pipeline --skip-postgres "$@"

echo "=== Pipeline complete ==="
