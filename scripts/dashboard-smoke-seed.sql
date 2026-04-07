BEGIN;

TRUNCATE TABLE
    jobq_job_logs,
    jobq_dashboard_audit_log,
    jobq_worker_nodes,
    jobq_queue_controls,
    jobq_jobs
RESTART IDENTITY CASCADE;

INSERT INTO jobq_jobs (
    id,
    type,
    payload,
    created_at,
    updated_at,
    locked_at,
    locked_by,
    processing_started_at,
    finished_at,
    failed_at,
    cancelled_at,
    error_message,
    progress_percent,
    progress_message,
    retry_count,
    max_retries,
    priority,
    run_at,
    group_id,
    replace_key,
    cron
)
VALUES
    (
        '11111111-1111-1111-1111-111111111111',
        'RECURRING_JOB',
        '{}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '2 minutes',
        CURRENT_TIMESTAMP - INTERVAL '2 minutes',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        3,
        0,
        CURRENT_TIMESTAMP - INTERVAL '30 seconds',
        NULL,
        '__jobq_recurring__:*/2 * * * * *',
        '*/2 * * * * *'
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        'com.example.jobs.DelayedReminderJob',
        '{"email":"later@example.com"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '25 minutes',
        CURRENT_TIMESTAMP - INTERVAL '25 minutes',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        4,
        0,
        CURRENT_TIMESTAMP + INTERVAL '15 minutes',
        'emails',
        'later-1',
        NULL
    ),
    (
        '33333333-3333-3333-3333-333333333333',
        'com.example.jobs.ImportJob',
        '{"file":"customers.csv"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
        CURRENT_TIMESTAMP - INTERVAL '1 minute',
        CURRENT_TIMESTAMP - INTERVAL '6 minutes',
        'node-smoke',
        CURRENT_TIMESTAMP - INTERVAL '29 minutes',
        NULL,
        NULL,
        NULL,
        NULL,
        65,
        'Validating batch 4/6',
        1,
        8,
        2,
        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
        'imports',
        'file-customers',
        NULL
    ),
    (
        '44444444-4444-4444-4444-444444444444',
        'com.example.jobs.SyncJob',
        '{"customer":"abc-123"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
        CURRENT_TIMESTAMP - INTERVAL '2 minutes',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        2,
        10,
        5,
        CURRENT_TIMESTAMP - INTERVAL '30 minutes',
        'sync',
        'sync-abc-123',
        NULL
    ),
    (
        '55555555-5555-5555-5555-555555555555',
        'com.example.jobs.CompletedEmailJob',
        '{"email":"done@example.com"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '40 minutes',
        CURRENT_TIMESTAMP - INTERVAL '10 minutes',
        CURRENT_TIMESTAMP - INTERVAL '39 minutes',
        'node-smoke',
        CURRENT_TIMESTAMP - INTERVAL '39 minutes',
        CURRENT_TIMESTAMP - INTERVAL '34 minutes',
        NULL,
        NULL,
        NULL,
        100,
        'Completed',
        1,
        5,
        3,
        CURRENT_TIMESTAMP - INTERVAL '40 minutes',
        'emails',
        'done-1',
        NULL
    ),
    (
        '66666666-6666-6666-6666-666666666666',
        'com.example.jobs.ReportJob',
        '{"customer":"cust-42"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '45 minutes',
        CURRENT_TIMESTAMP - INTERVAL '4 minutes',
        CURRENT_TIMESTAMP - INTERVAL '35 minutes',
        'node-smoke',
        CURRENT_TIMESTAMP - INTERVAL '35 minutes',
        NULL,
        CURRENT_TIMESTAMP - INTERVAL '24 minutes',
        NULL,
        'Downstream timeout while generating report',
        NULL,
        NULL,
        3,
        3,
        1,
        CURRENT_TIMESTAMP - INTERVAL '45 minutes',
        'reports',
        'cust-42',
        NULL
    ),
    (
        '77777777-7777-7777-7777-777777777777',
        'com.example.jobs.CleanupJob',
        '{"tenant":"tenant-7"}'::jsonb,
        CURRENT_TIMESTAMP - INTERVAL '4 hours',
        CURRENT_TIMESTAMP - INTERVAL '3 hours',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        CURRENT_TIMESTAMP - INTERVAL '3 hours',
        'Cancelled from dashboard',
        NULL,
        'Cancelled',
        0,
        2,
        0,
        CURRENT_TIMESTAMP - INTERVAL '4 hours',
        'maintenance',
        'tenant-7',
        NULL
    );

INSERT INTO jobq_job_logs (job_id, level, message, created_at)
VALUES
    (
        '33333333-3333-3333-3333-333333333333',
        'INFO',
        'Started import for customers.csv',
        CURRENT_TIMESTAMP - INTERVAL '29 minutes'
    ),
    (
        '33333333-3333-3333-3333-333333333333',
        'INFO',
        'Validated batch 4 of 6',
        CURRENT_TIMESTAMP - INTERVAL '2 minutes'
    ),
    (
        '66666666-6666-6666-6666-666666666666',
        'WARN',
        'Downstream report generator timed out',
        CURRENT_TIMESTAMP - INTERVAL '24 minutes'
    ),
    (
        '77777777-7777-7777-7777-777777777777',
        'INFO',
        'Cancelled from dashboard action',
        CURRENT_TIMESTAMP - INTERVAL '3 hours'
    );

INSERT INTO jobq_queue_controls (
    type,
    paused,
    max_concurrency,
    rate_limit_per_minute,
    dispatch_cooldown_ms,
    updated_at
)
VALUES
    (
        'com.example.jobs.ImportJob',
        TRUE,
        1,
        6,
        3000,
        CURRENT_TIMESTAMP - INTERVAL '5 minutes'
    ),
    (
        'com.example.jobs.ReportJob',
        FALSE,
        3,
        20,
        500,
        CURRENT_TIMESTAMP - INTERVAL '10 minutes'
    );

INSERT INTO jobq_worker_nodes (
    node_id,
    started_at,
    last_seen_at,
    worker_capacity,
    active_processing_count,
    queue_types,
    notify_enabled
)
VALUES
    (
        'node-smoke',
        CURRENT_TIMESTAMP - INTERVAL '6 hours',
        CURRENT_TIMESTAMP - INTERVAL '15 seconds',
        4,
        1,
        'com.example.jobs.ImportJob,com.example.jobs.ReportJob',
        TRUE
    ),
    (
        'node-warm-standby',
        CURRENT_TIMESTAMP - INTERVAL '2 hours',
        CURRENT_TIMESTAMP - INTERVAL '40 seconds',
        2,
        0,
        'RECURRING_JOB,com.example.jobs.DelayedReminderJob',
        TRUE
    );

INSERT INTO jobq_dashboard_audit_log (username, action, subject, details, created_at)
VALUES
    (
        'admin',
        'QUEUE_PAUSE',
        'com.example.jobs.ImportJob',
        'Paused queue with concurrency=1 rate=6/min',
        CURRENT_TIMESTAMP - INTERVAL '5 minutes'
    ),
    (
        'admin',
        'JOB_CANCEL',
        '77777777-7777-7777-7777-777777777777',
        'Cancelled from dashboard smoke seed',
        CURRENT_TIMESTAMP - INTERVAL '3 hours'
    );

COMMIT;
