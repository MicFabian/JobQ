-- Ensure core lifecycle timestamps always have defaults, even when tables were
-- initially created externally (e.g., by Hibernate ddl-auto=update).
ALTER TABLE jobq_jobs
    ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN run_at SET DEFAULT CURRENT_TIMESTAMP;

-- Backfill historic rows where one or more timestamp columns were left NULL.
UPDATE jobq_jobs
SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
    updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP),
    run_at = COALESCE(run_at, created_at, CURRENT_TIMESTAMP)
WHERE created_at IS NULL
   OR updated_at IS NULL
   OR run_at IS NULL;

ALTER TABLE jobq_jobs
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN run_at SET NOT NULL;
