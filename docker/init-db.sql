-- PostgreSQL initialization for fraud analytics serving layer

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    layer_stats TEXT,
    duration_seconds DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created ON pipeline_runs(created_at DESC);

COMMENT ON TABLE pipeline_runs IS 'Audit log for fraud detection pipeline executions';
