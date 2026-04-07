ALTER TABLE jobq_jobs
    SET (
        fillfactor = 85,
        autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_analyze_scale_factor = 0.01,
        autovacuum_vacuum_threshold = 50,
        autovacuum_analyze_threshold = 50
    );

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_processing_locked_at
    ON jobq_jobs(type, locked_at)
    WHERE processing_started_at IS NOT NULL
      AND finished_at IS NULL
      AND failed_at IS NULL
      AND locked_at IS NOT NULL;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_type_trgm
    ON jobq_jobs USING gin (lower(type) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_group_trgm
    ON jobq_jobs USING gin (lower(COALESCE(group_id, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_replace_key_trgm
    ON jobq_jobs USING gin (lower(COALESCE(replace_key, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobq_jobs_error_message_trgm
    ON jobq_jobs USING gin (lower(COALESCE(error_message, '')) gin_trgm_ops);
