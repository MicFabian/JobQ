CREATE TABLE IF NOT EXISTS jobq_jobs (
    id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    payload JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    locked_at TIMESTAMP WITH TIME ZONE,
    locked_by VARCHAR(255),
    error_message TEXT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    priority INT DEFAULT 0,
    run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_polling ON jobq_jobs(status, type, run_at, priority DESC);
