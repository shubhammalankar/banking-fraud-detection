FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY src/ src/
COPY scripts/ scripts/
COPY tests/ tests/

ENV PYTHONPATH=/app/src
ENV SPARK_MASTER=local[*]

RUN mkdir -p /app/data/raw/transactions /app/data/delta /app/data/checkpoints

CMD ["python", "-m", "fraud_detection.etl.pipeline", "--full-refresh", "--skip-postgres"]