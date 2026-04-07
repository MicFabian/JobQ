ALTER TABLE jobq_jobs
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE jobq_jobs
    ADD COLUMN IF NOT EXISTS progress_percent INT;

ALTER TABLE jobq_jobs
    ADD COLUMN IF NOT EXISTS progress_message TEXT;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_cancelled_created_at
    ON jobq_jobs(cancelled_at DESC, created_at DESC)
    WHERE cancelled_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_active_type_started_at
    ON jobq_jobs(type, processing_started_at DESC, created_at DESC)
    WHERE processing_started_at IS NOT NULL
      AND finished_at IS NULL
      AND failed_at IS NULL
      AND cancelled_at IS NULL;

CREATE TABLE IF NOT EXISTS jobq_queue_controls (
    type VARCHAR(255) PRIMARY KEY,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrency INT,
    rate_limit_per_minute INT,
    dispatch_cooldown_ms INT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobq_queue_controls_updated_at
    ON jobq_queue_controls(updated_at DESC);

CREATE TABLE IF NOT EXISTS jobq_job_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobq_jobs(id) ON DELETE CASCADE,
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobq_job_logs_job_id_id
    ON jobq_job_logs(job_id, id DESC);

CREATE TABLE IF NOT EXISTS jobq_worker_nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    worker_capacity INT NOT NULL,
    active_processing_count INT NOT NULL DEFAULT 0,
    queue_types TEXT,
    notify_enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_jobq_worker_nodes_last_seen_at
    ON jobq_worker_nodes(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS jobq_dashboard_audit_log (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    action VARCHAR(120) NOT NULL,
    subject VARCHAR(255),
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobq_dashboard_audit_log_created_at
    ON jobq_dashboard_audit_log(created_at DESC);
