CREATE INDEX IF NOT EXISTS idx_jobq_jobs_active_type_group_run_at
    ON jobq_jobs(type, group_id, run_at)
    WHERE group_id IS NOT NULL
      AND processing_started_at IS NULL
      AND finished_at IS NULL
      AND failed_at IS NULL;
