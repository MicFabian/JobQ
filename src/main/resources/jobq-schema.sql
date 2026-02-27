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
    error_message TEXT,
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
ALTER TABLE jobq_jobs DROP COLUMN IF EXISTS status;

DROP INDEX IF EXISTS idx_jobq_jobs_polling;
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_polling
    ON jobq_jobs(type, run_at, priority DESC, created_at)
    WHERE processing_started_at IS NULL AND finished_at IS NULL AND failed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_group ON jobq_jobs(group_id) WHERE group_id IS NOT NULL;
DROP INDEX IF EXISTS idx_jobq_jobs_replace_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobq_jobs_replace_key
    ON jobq_jobs(type, replace_key)
    WHERE finished_at IS NULL
      AND failed_at IS NULL
      AND replace_key IS NOT NULL;
