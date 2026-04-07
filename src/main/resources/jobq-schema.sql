CREATE TABLE IF NOT EXISTS jobq_jobs (
    id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    locked_at TIMESTAMP WITH TIME ZONE,
    locked_by VARCHAR(255),
    processing_started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    progress_percent INT,
    progress_message TEXT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    priority INT DEFAULT 0,
    run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    group_id TEXT,
    replace_key TEXT,
    cron TEXT
);

-- Ensure columns exist for existing tables
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS failed_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS group_id TEXT;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS replace_key TEXT;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS cron TEXT;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS progress_percent INT;
ALTER TABLE jobq_jobs ADD COLUMN IF NOT EXISTS progress_message TEXT;
ALTER TABLE jobq_jobs DROP COLUMN IF EXISTS status;

DROP INDEX IF EXISTS idx_jobq_jobs_polling;
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_polling
    ON jobq_jobs(type, run_at, priority DESC, created_at)
    WHERE processing_started_at IS NULL AND finished_at IS NULL AND failed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_group ON jobq_jobs(group_id) WHERE group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_active_type_group_run_at
    ON jobq_jobs(type, group_id, run_at)
    WHERE group_id IS NOT NULL
      AND processing_started_at IS NULL
      AND finished_at IS NULL
      AND failed_at IS NULL;
DROP INDEX IF EXISTS idx_jobq_jobs_replace_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobq_jobs_replace_key
    ON jobq_jobs(type, replace_key)
    WHERE processing_started_at IS NULL
      AND finished_at IS NULL
      AND failed_at IS NULL
      AND replace_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_created_at_desc
    ON jobq_jobs(created_at DESC);

DROP INDEX IF EXISTS idx_jobq_jobs_active_created_at;
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_pending_created_at
    ON jobq_jobs(created_at DESC)
    WHERE finished_at IS NULL
      AND failed_at IS NULL
      AND processing_started_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_processing_created_at
    ON jobq_jobs(created_at DESC)
    WHERE finished_at IS NULL
      AND failed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_completed_created_at
    ON jobq_jobs(created_at DESC)
    WHERE finished_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_failed_created_at
    ON jobq_jobs(created_at DESC)
    WHERE failed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_finished_at_cleanup
    ON jobq_jobs(finished_at)
    WHERE finished_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_failed_at_cleanup
    ON jobq_jobs(failed_at)
    WHERE failed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_cancelled_created_at
    ON jobq_jobs(cancelled_at DESC, created_at DESC)
    WHERE cancelled_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_active_type_cron
    ON jobq_jobs(type, cron)
    WHERE finished_at IS NULL
      AND failed_at IS NULL
      AND cron IS NOT NULL;

CREATE TABLE IF NOT EXISTS jobq_queue_controls (
    type VARCHAR(255) PRIMARY KEY,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrency INT,
    rate_limit_per_minute INT,
    dispatch_cooldown_ms INT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS jobq_dashboard_audit_log (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    action VARCHAR(120) NOT NULL,
    subject VARCHAR(255),
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
