package com.jobq.internal;

import com.jobq.config.JobQProperties;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class JobOperationsService {

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final int DEFAULT_METRIC_POINTS = 24;

    private final JdbcTemplate jdbcTemplate;
    private final String jobTableName;
    private final String queueControlTableName;
    private final String jobLogTableName;
    private final String workerNodeTableName;
    private final String auditTableName;

    public JobOperationsService(JdbcTemplate jdbcTemplate, JobQProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        String prefix = properties.getDatabase().getTablePrefix();
        this.jobTableName = resolveTableName(prefix, "jobq_jobs");
        this.queueControlTableName = resolveTableName(prefix, "jobq_queue_controls");
        this.jobLogTableName = resolveTableName(prefix, "jobq_job_logs");
        this.workerNodeTableName = resolveTableName(prefix, "jobq_worker_nodes");
        this.auditTableName = resolveTableName(prefix, "jobq_dashboard_audit_log");
    }

    public Map<String, QueueRuntimeState> loadQueueRuntimeStates() {
        String sql =
                """
                SELECT type, paused, max_concurrency, rate_limit_per_minute, dispatch_cooldown_ms, updated_at
                FROM %s
                """
                        .formatted(queueControlTableName);
        List<QueueRuntimeState> rows = jdbcTemplate.query(sql, queueRuntimeStateRowMapper());
        Map<String, QueueRuntimeState> states = new LinkedHashMap<>(rows.size());
        for (QueueRuntimeState row : rows) {
            states.put(row.type(), row);
        }
        return Map.copyOf(states);
    }

    public QueueRuntimeState upsertQueueControl(
            String type,
            boolean paused,
            Integer maxConcurrency,
            Integer rateLimitPerMinute,
            Integer dispatchCooldownMs,
            OffsetDateTime now) {
        String normalizedType = normalizeRequiredType(type);
        Integer normalizedMaxConcurrency = normalizePositiveOverride(maxConcurrency, "maxConcurrency");
        Integer normalizedRateLimit = normalizePositiveOverride(rateLimitPerMinute, "rateLimitPerMinute");
        Integer normalizedDispatchCooldown = normalizeNonNegativeOverride(dispatchCooldownMs, "dispatchCooldownMs");

        String sql =
                """
                INSERT INTO %s (type, paused, max_concurrency, rate_limit_per_minute, dispatch_cooldown_ms, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (type)
                DO UPDATE SET
                    paused = EXCLUDED.paused,
                    max_concurrency = EXCLUDED.max_concurrency,
                    rate_limit_per_minute = EXCLUDED.rate_limit_per_minute,
                    dispatch_cooldown_ms = EXCLUDED.dispatch_cooldown_ms,
                    updated_at = EXCLUDED.updated_at
                """
                        .formatted(queueControlTableName);
        jdbcTemplate.update(
                sql,
                normalizedType,
                paused,
                normalizedMaxConcurrency,
                normalizedRateLimit,
                normalizedDispatchCooldown,
                now);
        return new QueueRuntimeState(
                normalizedType, paused, normalizedMaxConcurrency, normalizedRateLimit, normalizedDispatchCooldown, now);
    }

    public List<QueueStats> loadQueueStats(
            Collection<String> registeredJobTypes,
            Map<String, JobTypeMetadataRegistry.RecurringJobMetadata> recurringJobs) {
        Map<String, QueueRuntimeState> controlsByType = new LinkedHashMap<>(loadQueueRuntimeStates());
        String sql =
                """
                SELECT
                    type,
                    COALESCE(SUM(CASE
                        WHEN processing_started_at IS NULL
                         AND finished_at IS NULL
                         AND failed_at IS NULL
                         AND cancelled_at IS NULL
                        THEN 1 ELSE 0 END), 0) AS pending_count,
                    COALESCE(SUM(CASE
                        WHEN processing_started_at IS NOT NULL
                         AND finished_at IS NULL
                         AND failed_at IS NULL
                         AND cancelled_at IS NULL
                        THEN 1 ELSE 0 END), 0) AS processing_count,
                    COALESCE(SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS completed_count,
                    COALESCE(SUM(CASE WHEN failed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS failed_count,
                    COALESCE(SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS cancelled_count,
                    MIN(CASE
                        WHEN processing_started_at IS NULL
                         AND finished_at IS NULL
                         AND failed_at IS NULL
                         AND cancelled_at IS NULL
                        THEN run_at
                        ELSE NULL
                    END) AS next_run_at,
                    MAX(COALESCE(finished_at, failed_at, cancelled_at)) AS last_terminal_at
                FROM %s
                GROUP BY type
                """
                        .formatted(jobTableName);
        Map<String, QueueAggregate> aggregates = new LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String type = rs.getString("type");
            aggregates.put(
                    type,
                    new QueueAggregate(
                            type,
                            rs.getLong("pending_count"),
                            rs.getLong("processing_count"),
                            rs.getLong("completed_count"),
                            rs.getLong("failed_count"),
                            rs.getLong("cancelled_count"),
                            rs.getObject("next_run_at", OffsetDateTime.class),
                            rs.getObject("last_terminal_at", OffsetDateTime.class)));
        });

        Map<String, QueueStats> result = new LinkedHashMap<>();
        for (String type : registeredJobTypes) {
            QueueAggregate aggregate = aggregates.getOrDefault(type, QueueAggregate.empty(type));
            QueueRuntimeState control = controlsByType.getOrDefault(type, QueueRuntimeState.defaults(type));
            boolean recurring = recurringJobs.containsKey(type);
            result.put(type, new QueueStats(aggregate, control, recurring));
        }
        for (Map.Entry<String, QueueAggregate> entry : aggregates.entrySet()) {
            result.computeIfAbsent(
                    entry.getKey(),
                    type -> new QueueStats(
                            entry.getValue(),
                            controlsByType.getOrDefault(type, QueueRuntimeState.defaults(type)),
                            recurringJobs.containsKey(type)));
        }
        return result.values().stream()
                .sorted(Comparator.comparing(QueueStats::type, String.CASE_INSENSITIVE_ORDER))
                .toList();
    }

    public void heartbeatNode(
            String nodeId,
            OffsetDateTime startedAt,
            OffsetDateTime lastSeenAt,
            int workerCapacity,
            int activeProcessingCount,
            Collection<String> queueTypes,
            boolean notifyEnabled) {
        String sql =
                """
                INSERT INTO %s (node_id, started_at, last_seen_at, worker_capacity, active_processing_count, queue_types, notify_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (node_id)
                DO UPDATE SET
                    last_seen_at = EXCLUDED.last_seen_at,
                    worker_capacity = EXCLUDED.worker_capacity,
                    active_processing_count = EXCLUDED.active_processing_count,
                    queue_types = EXCLUDED.queue_types,
                    notify_enabled = EXCLUDED.notify_enabled
                """
                        .formatted(workerNodeTableName);
        jdbcTemplate.update(
                sql,
                nodeId,
                startedAt,
                lastSeenAt,
                workerCapacity,
                activeProcessingCount,
                String.join(",", queueTypes),
                notifyEnabled);
    }

    public List<WorkerNodeStatus> loadWorkerNodes(Duration staleAfter) {
        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(staleAfter);
        String sql =
                """
                SELECT node_id, started_at, last_seen_at, worker_capacity, active_processing_count, queue_types, notify_enabled
                FROM %s
                WHERE last_seen_at >= ?
                ORDER BY last_seen_at DESC, node_id ASC
                """
                        .formatted(workerNodeTableName);
        return jdbcTemplate.query(sql, workerNodeStatusRowMapper(), threshold);
    }

    public int cancelJob(UUID id, String reason, OffsetDateTime now) {
        String sql =
                """
                UPDATE %s
                SET cancelled_at = ?,
                    updated_at = ?,
                    error_message = ?,
                    progress_message = COALESCE(progress_message, ?)
                WHERE id = ?
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND cancelled_at IS NULL
                """
                        .formatted(jobTableName);
        return jdbcTemplate.update(sql, now, now, truncateMessage(reason), "Cancelled", id);
    }

    public int cancelJobsByFilter(
            String status,
            String query,
            boolean scheduledOnly,
            boolean retriedOnly,
            OffsetDateTime now,
            String reason) {
        FilterClause clause = buildFilterClause(status, query, scheduledOnly, retriedOnly, FilterScope.ACTIVE_ANY);
        String sql =
                """
                UPDATE %s
                SET cancelled_at = ?,
                    updated_at = ?,
                    error_message = ?,
                    progress_message = COALESCE(progress_message, ?)
                WHERE %s
                """
                        .formatted(jobTableName, clause.sql());
        List<Object> params = new ArrayList<>();
        params.add(now);
        params.add(now);
        params.add(truncateMessage(reason));
        params.add("Cancelled");
        params.addAll(clause.params());
        return jdbcTemplate.update(sql, params.toArray());
    }

    public int runDelayedJobsByFilterNow(
            String status, String query, boolean scheduledOnly, boolean retriedOnly, OffsetDateTime now) {
        FilterClause clause = buildFilterClause(status, query, scheduledOnly, retriedOnly, FilterScope.DELAYED_PENDING);
        String sql =
                """
                UPDATE %s
                SET run_at = ?,
                    updated_at = ?
                WHERE %s
                """
                        .formatted(jobTableName, clause.sql());
        List<Object> params = new ArrayList<>();
        params.add(now);
        params.add(now);
        params.addAll(clause.params());
        return jdbcTemplate.update(sql, params.toArray());
    }

    public int rerunTerminalJobsByFilter(
            String status, String query, boolean scheduledOnly, boolean retriedOnly, OffsetDateTime now) {
        FilterClause clause = buildFilterClause(status, query, scheduledOnly, retriedOnly, FilterScope.TERMINAL);
        String sql =
                """
                UPDATE %s
                SET processing_started_at = NULL,
                    finished_at = NULL,
                    failed_at = NULL,
                    cancelled_at = NULL,
                    retry_count = 0,
                    error_message = NULL,
                    locked_at = NULL,
                    locked_by = NULL,
                    run_at = ?,
                    updated_at = ?,
                    progress_percent = NULL,
                    progress_message = NULL
                WHERE %s
                """
                        .formatted(jobTableName, clause.sql());
        List<Object> params = new ArrayList<>();
        params.add(now);
        params.add(now);
        params.addAll(clause.params());
        return jdbcTemplate.update(sql, params.toArray());
    }

    public long appendJobLog(UUID jobId, String level, String message) {
        String normalizedLevel = normalizeLogLevel(level);
        String normalizedMessage = message == null ? "" : message.strip();
        if (normalizedMessage.isEmpty()) {
            return -1L;
        }

        String sql =
                """
                INSERT INTO %s (job_id, level, message)
                VALUES (?, ?, ?)
                RETURNING id
                """
                        .formatted(jobLogTableName);
        Long id = jdbcTemplate.query(
                sql,
                ps -> {
                    ps.setObject(1, jobId);
                    ps.setString(2, normalizedLevel);
                    ps.setString(3, truncateMessage(normalizedMessage));
                },
                rs -> rs.next() ? rs.getLong("id") : null);
        return id == null ? -1L : id;
    }

    public List<JobLogEntry> loadJobLogs(UUID jobId, long afterId, int limit) {
        int normalizedLimit = Math.max(1, Math.min(500, limit));
        String sql =
                """
                SELECT id, job_id, level, message, created_at
                FROM %s
                WHERE job_id = ?
                  AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """
                        .formatted(jobLogTableName);
        return jdbcTemplate.query(sql, jobLogEntryRowMapper(), jobId, afterId, normalizedLimit);
    }

    public List<JobLogEntry> loadLatestJobLogs(UUID jobId, int limit) {
        int normalizedLimit = Math.max(1, Math.min(500, limit));
        String sql =
                """
                SELECT id, job_id, level, message, created_at
                FROM (
                    SELECT id, job_id, level, message, created_at
                    FROM %s
                    WHERE job_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                ) recent_logs
                ORDER BY id ASC
                """
                        .formatted(jobLogTableName);
        return jdbcTemplate.query(sql, jobLogEntryRowMapper(), jobId, normalizedLimit);
    }

    public DashboardMetricsSnapshot loadDashboardMetrics(Duration lookback) {
        Duration effectiveLookback =
                lookback == null || lookback.isNegative() || lookback.isZero() ? Duration.ofHours(6) : lookback;
        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(effectiveLookback);

        String percentileSql =
                """
                SELECT
                    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processing_started_at - created_at)) * 1000), 0) AS queue_p50_ms,
                    COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processing_started_at - created_at)) * 1000), 0) AS queue_p95_ms,
                    COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processing_started_at - created_at)) * 1000), 0) AS queue_p99_ms,
                    COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (terminal_at - processing_started_at)) * 1000), 0) AS runtime_p50_ms,
                    COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (terminal_at - processing_started_at)) * 1000), 0) AS runtime_p95_ms,
                    COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (terminal_at - processing_started_at)) * 1000), 0) AS runtime_p99_ms,
                    COUNT(*) AS terminal_count,
                    COALESCE(SUM(CASE WHEN failed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS failed_count,
                    COALESCE(SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS completed_count,
                    COALESCE(SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS cancelled_count
                FROM (
                    SELECT
                        created_at,
                        processing_started_at,
                        finished_at,
                        failed_at,
                        cancelled_at,
                        COALESCE(finished_at, failed_at, cancelled_at) AS terminal_at
                    FROM %s
                    WHERE processing_started_at IS NOT NULL
                      AND COALESCE(finished_at, failed_at, cancelled_at) >= ?
                ) recent_jobs
                """
                        .formatted(jobTableName);

        MetricsRollup rollup = jdbcTemplate.query(
                percentileSql,
                ps -> ps.setObject(1, threshold),
                rs -> rs.next()
                        ? new MetricsRollup(
                                rs.getDouble("queue_p50_ms"),
                                rs.getDouble("queue_p95_ms"),
                                rs.getDouble("queue_p99_ms"),
                                rs.getDouble("runtime_p50_ms"),
                                rs.getDouble("runtime_p95_ms"),
                                rs.getDouble("runtime_p99_ms"),
                                rs.getLong("terminal_count"),
                                rs.getLong("failed_count"),
                                rs.getLong("completed_count"),
                                rs.getLong("cancelled_count"))
                        : MetricsRollup.empty());

        String seriesSql =
                """
                SELECT
                    date_trunc('minute', COALESCE(finished_at, failed_at, cancelled_at)) AS bucket,
                    COALESCE(SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS completed_count,
                    COALESCE(SUM(CASE WHEN failed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS failed_count,
                    COALESCE(SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS cancelled_count
                FROM %s
                WHERE COALESCE(finished_at, failed_at, cancelled_at) >= ?
                GROUP BY bucket
                ORDER BY bucket ASC
                """
                        .formatted(jobTableName);

        List<MetricsPoint> points = jdbcTemplate.query(
                seriesSql,
                (rs, rowNum) -> new MetricsPoint(
                        rs.getObject("bucket", OffsetDateTime.class),
                        rs.getLong("completed_count"),
                        rs.getLong("failed_count"),
                        rs.getLong("cancelled_count")),
                threshold);

        double lookbackMinutes = Math.max(1.0, effectiveLookback.toMinutes());
        double throughputPerMinute = rollup.terminalCount() / lookbackMinutes;
        double failureRate = rollup.terminalCount() == 0
                ? 0.0
                : ((double) rollup.failedCount() / (double) rollup.terminalCount()) * 100.0;

        return new DashboardMetricsSnapshot(
                lookback,
                rollup.queueP50Ms(),
                rollup.queueP95Ms(),
                rollup.queueP99Ms(),
                rollup.runtimeP50Ms(),
                rollup.runtimeP95Ms(),
                rollup.runtimeP99Ms(),
                throughputPerMinute,
                failureRate,
                rollup.completedCount(),
                rollup.failedCount(),
                rollup.cancelledCount(),
                padMetricsPoints(points, effectiveLookback));
    }

    public List<AuditEntry> loadRecentAuditEntries(int limit) {
        int normalizedLimit = Math.max(1, Math.min(200, limit));
        String sql =
                """
                SELECT id, username, action, subject, details, created_at
                FROM %s
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """
                        .formatted(auditTableName);
        return jdbcTemplate.query(sql, auditEntryRowMapper(), normalizedLimit);
    }

    public void recordAudit(String username, String action, String subject, String details) {
        String normalizedUsername = username == null || username.isBlank() ? "anonymous" : username.trim();
        String normalizedAction =
                action == null || action.isBlank() ? "UNKNOWN" : action.trim().toUpperCase(Locale.ROOT);
        String sql =
                """
                INSERT INTO %s (username, action, subject, details)
                VALUES (?, ?, ?, ?)
                """
                        .formatted(auditTableName);
        jdbcTemplate.update(
                sql,
                truncateMessage(normalizedUsername),
                truncateMessage(normalizedAction),
                subject == null ? null : truncateMessage(subject),
                details == null ? null : truncateMessage(details));
    }

    public List<RecurringSchedule> loadRecurringSchedules(
            Map<String, JobTypeMetadataRegistry.RecurringJobMetadata> recurringMetadata) {
        if (recurringMetadata.isEmpty()) {
            return List.of();
        }

        String sql =
                """
                SELECT
                    type,
                    cron,
                    MIN(CASE
                        WHEN finished_at IS NULL
                         AND failed_at IS NULL
                         AND cancelled_at IS NULL
                        THEN run_at
                        ELSE NULL
                    END) AS next_run_at,
                    MAX(CASE
                        WHEN finished_at IS NOT NULL THEN finished_at
                        WHEN failed_at IS NOT NULL THEN failed_at
                        WHEN cancelled_at IS NOT NULL THEN cancelled_at
                        ELSE NULL
                    END) AS last_outcome_at,
                    COALESCE(SUM(CASE
                        WHEN finished_at IS NULL
                         AND failed_at IS NULL
                         AND cancelled_at IS NULL
                        THEN 1 ELSE 0 END), 0) AS active_count
                FROM %s
                WHERE cron IS NOT NULL
                GROUP BY type, cron
                """
                        .formatted(jobTableName);
        Map<String, RecurringAggregate> aggregates = new LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String type = rs.getString("type");
            String cron = rs.getString("cron");
            aggregates.put(
                    type,
                    new RecurringAggregate(
                            type,
                            cron,
                            rs.getObject("next_run_at", OffsetDateTime.class),
                            rs.getObject("last_outcome_at", OffsetDateTime.class),
                            rs.getLong("active_count")));
        });

        List<RecurringSchedule> schedules = new ArrayList<>(recurringMetadata.size());
        for (JobTypeMetadataRegistry.RecurringJobMetadata metadata : recurringMetadata.values()) {
            RecurringAggregate aggregate = aggregates.getOrDefault(
                    metadata.type(), RecurringAggregate.empty(metadata.type(), metadata.cron()));
            schedules.add(new RecurringSchedule(metadata, aggregate));
        }
        schedules.sort(Comparator.comparing(RecurringSchedule::type, String.CASE_INSENSITIVE_ORDER));
        return List.copyOf(schedules);
    }

    private List<MetricsPoint> padMetricsPoints(List<MetricsPoint> points, Duration lookback) {
        if (points.size() >= DEFAULT_METRIC_POINTS) {
            return points;
        }
        OffsetDateTime bucket =
                OffsetDateTime.now(ZoneOffset.UTC).minus(lookback).withSecond(0).withNano(0);
        Map<OffsetDateTime, MetricsPoint> pointsByBucket = new LinkedHashMap<>();
        for (MetricsPoint point : points) {
            if (point.bucket() != null) {
                pointsByBucket.put(point.bucket(), point);
            }
        }
        List<MetricsPoint> padded = new ArrayList<>(DEFAULT_METRIC_POINTS);
        for (int i = 0; i < DEFAULT_METRIC_POINTS; i++) {
            MetricsPoint point = pointsByBucket.getOrDefault(bucket, new MetricsPoint(bucket, 0, 0, 0));
            padded.add(point);
            bucket = bucket.plusMinutes(
                    Math.max(1, Math.floorDiv(Math.max(1, lookback.toMinutes()), DEFAULT_METRIC_POINTS)));
        }
        return List.copyOf(padded);
    }

    private FilterClause buildFilterClause(
            String status, String query, boolean scheduledOnly, boolean retriedOnly, FilterScope scope) {
        String normalizedStatus = normalizeStatus(status);
        String normalizedQuery = query == null ? "" : query.trim();
        UUID jobId = tryParseUuid(normalizedQuery);

        List<String> clauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        switch (scope) {
            case ACTIVE_ANY -> clauses.add("finished_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL");
            case DELAYED_PENDING ->
                clauses.add(
                        "processing_started_at IS NULL AND finished_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL AND run_at > ?");
            case TERMINAL ->
                clauses.add("(finished_at IS NOT NULL OR failed_at IS NOT NULL OR cancelled_at IS NOT NULL)");
        }

        if (scope == FilterScope.DELAYED_PENDING) {
            params.add(OffsetDateTime.now(ZoneOffset.UTC));
        }

        if (!normalizedStatus.isBlank()) {
            switch (normalizedStatus) {
                case "PENDING" ->
                    clauses.add(
                            "processing_started_at IS NULL AND finished_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL");
                case "PROCESSING" ->
                    clauses.add(
                            "processing_started_at IS NOT NULL AND finished_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL");
                case "COMPLETED" -> clauses.add("finished_at IS NOT NULL");
                case "FAILED" -> clauses.add("failed_at IS NOT NULL");
                case "CANCELLED" -> clauses.add("cancelled_at IS NOT NULL");
                default -> {
                    // ignore
                }
            }
        }

        if (scheduledOnly) {
            clauses.add("run_at > ?");
            params.add(OffsetDateTime.now(ZoneOffset.UTC));
        }
        if (retriedOnly) {
            clauses.add("retry_count > 0");
        }

        if (!normalizedQuery.isBlank()) {
            clauses.add(
                    """
                    (
                        LOWER(type) LIKE LOWER(?)
                        OR LOWER(COALESCE(group_id, '')) LIKE LOWER(?)
                        OR LOWER(COALESCE(replace_key, '')) LIKE LOWER(?)
                        OR LOWER(COALESCE(error_message, '')) LIKE LOWER(?)
                        OR CAST(id AS TEXT) = ?
                    )
                    """);
            String likeValue = "%" + normalizedQuery + "%";
            params.add(likeValue);
            params.add(likeValue);
            params.add(likeValue);
            params.add(likeValue);
            params.add(jobId == null ? "00000000-0000-0000-0000-000000000000" : jobId.toString());
        }

        return new FilterClause(String.join(" AND ", clauses), List.copyOf(params));
    }

    private String resolveTableName(String prefix, String logicalName) {
        String normalizedPrefix = prefix == null ? "" : prefix.trim();
        String tableName = normalizedPrefix + logicalName;
        if (!SAFE_TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Unsupported JobQ table name: " + tableName);
        }
        return tableName;
    }

    private String normalizeRequiredType(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Queue type must not be blank");
        }
        return type.trim();
    }

    private Integer normalizePositiveOverride(Integer value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be > 0 when set");
        }
        return value;
    }

    private Integer normalizeNonNegativeOverride(Integer value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be >= 0 when set");
        }
        return value;
    }

    private String normalizeLogLevel(String level) {
        if (level == null || level.isBlank()) {
            return "INFO";
        }
        return switch (level.trim().toUpperCase(Locale.ROOT)) {
            case "TRACE", "DEBUG", "INFO", "WARN", "ERROR" -> level.trim().toUpperCase(Locale.ROOT);
            default -> "INFO";
        };
    }

    private String truncateMessage(String message) {
        if (message == null) {
            return null;
        }
        String normalized = message.strip();
        return normalized.length() > 2_000 ? normalized.substring(0, 2_000) : normalized;
    }

    private String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return "";
        }
        return status.trim().toUpperCase(Locale.ROOT);
    }

    private UUID tryParseUuid(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value.trim());
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private RowMapper<QueueRuntimeState> queueRuntimeStateRowMapper() {
        return (rs, rowNum) -> new QueueRuntimeState(
                rs.getString("type"),
                rs.getBoolean("paused"),
                rs.getObject("max_concurrency", Integer.class),
                rs.getObject("rate_limit_per_minute", Integer.class),
                rs.getObject("dispatch_cooldown_ms", Integer.class),
                rs.getObject("updated_at", OffsetDateTime.class));
    }

    private RowMapper<JobLogEntry> jobLogEntryRowMapper() {
        return (rs, rowNum) -> new JobLogEntry(
                rs.getLong("id"),
                rs.getObject("job_id", UUID.class),
                rs.getString("level"),
                rs.getString("message"),
                rs.getObject("created_at", OffsetDateTime.class));
    }

    private RowMapper<WorkerNodeStatus> workerNodeStatusRowMapper() {
        return (rs, rowNum) -> new WorkerNodeStatus(
                rs.getString("node_id"),
                rs.getObject("started_at", OffsetDateTime.class),
                rs.getObject("last_seen_at", OffsetDateTime.class),
                rs.getInt("worker_capacity"),
                rs.getInt("active_processing_count"),
                parseQueueTypes(rs.getString("queue_types")),
                rs.getBoolean("notify_enabled"));
    }

    private RowMapper<AuditEntry> auditEntryRowMapper() {
        return (rs, rowNum) -> new AuditEntry(
                rs.getLong("id"),
                rs.getString("username"),
                rs.getString("action"),
                rs.getString("subject"),
                rs.getString("details"),
                rs.getObject("created_at", OffsetDateTime.class));
    }

    private List<String> parseQueueTypes(String rawQueueTypes) {
        if (rawQueueTypes == null || rawQueueTypes.isBlank()) {
            return List.of();
        }
        String[] parts = rawQueueTypes.split(",");
        List<String> queueTypes = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (part != null && !part.isBlank()) {
                queueTypes.add(part.trim());
            }
        }
        return List.copyOf(queueTypes);
    }

    public record QueueRuntimeState(
            String type,
            boolean paused,
            Integer maxConcurrency,
            Integer rateLimitPerMinute,
            Integer dispatchCooldownMs,
            OffsetDateTime updatedAt) {
        public static QueueRuntimeState defaults(String type) {
            return new QueueRuntimeState(type, false, null, null, null, null);
        }
    }

    public record QueueStats(
            String type,
            long pendingCount,
            long processingCount,
            long completedCount,
            long failedCount,
            long cancelledCount,
            OffsetDateTime nextRunAt,
            OffsetDateTime lastTerminalAt,
            boolean paused,
            Integer maxConcurrency,
            Integer rateLimitPerMinute,
            Integer dispatchCooldownMs,
            boolean recurring) {
        private QueueStats(QueueAggregate aggregate, QueueRuntimeState control, boolean recurring) {
            this(
                    aggregate.type(),
                    aggregate.pendingCount(),
                    aggregate.processingCount(),
                    aggregate.completedCount(),
                    aggregate.failedCount(),
                    aggregate.cancelledCount(),
                    aggregate.nextRunAt(),
                    aggregate.lastTerminalAt(),
                    control.paused(),
                    control.maxConcurrency(),
                    control.rateLimitPerMinute(),
                    control.dispatchCooldownMs(),
                    recurring);
        }

        public boolean hasActiveWork() {
            return pendingCount + processingCount > 0;
        }

        public boolean hasFailures() {
            return failedCount > 0;
        }

        public long activeBacklog() {
            return pendingCount + processingCount + failedCount;
        }

        public long totalCount() {
            return pendingCount + processingCount + completedCount + failedCount + cancelledCount;
        }

        public boolean hasControls() {
            return paused || maxConcurrency != null || rateLimitPerMinute != null || dispatchCooldownMs != null;
        }

        public boolean isInteresting() {
            return hasActiveWork() || hasFailures() || hasControls() || recurring;
        }
    }

    public record WorkerNodeStatus(
            String nodeId,
            OffsetDateTime startedAt,
            OffsetDateTime lastSeenAt,
            int workerCapacity,
            int activeProcessingCount,
            List<String> queueTypes,
            boolean notifyEnabled) {}

    public record JobLogEntry(long id, UUID jobId, String level, String message, OffsetDateTime createdAt) {}

    public record DashboardMetricsSnapshot(
            Duration lookback,
            double queueP50Ms,
            double queueP95Ms,
            double queueP99Ms,
            double runtimeP50Ms,
            double runtimeP95Ms,
            double runtimeP99Ms,
            double throughputPerMinute,
            double failureRatePercent,
            long completedCount,
            long failedCount,
            long cancelledCount,
            List<MetricsPoint> points) {}

    public record MetricsPoint(OffsetDateTime bucket, long completedCount, long failedCount, long cancelledCount) {}

    public record AuditEntry(
            long id, String username, String action, String subject, String details, OffsetDateTime createdAt) {}

    public record RecurringSchedule(
            String type,
            String cron,
            com.jobq.annotation.Job.CronMisfirePolicy misfirePolicy,
            int maxCatchUpExecutions,
            Integer maxRetries,
            OffsetDateTime nextRunAt,
            OffsetDateTime lastOutcomeAt,
            long activeCount) {
        private RecurringSchedule(JobTypeMetadataRegistry.RecurringJobMetadata metadata, RecurringAggregate aggregate) {
            this(
                    metadata.type(),
                    metadata.cron(),
                    metadata.cronMisfirePolicy(),
                    metadata.maxCatchUpExecutions(),
                    metadata.maxRetries(),
                    aggregate.nextRunAt(),
                    aggregate.lastOutcomeAt(),
                    aggregate.activeCount());
        }
    }

    private record FilterClause(String sql, List<Object> params) {}

    private enum FilterScope {
        ACTIVE_ANY,
        DELAYED_PENDING,
        TERMINAL
    }

    private record QueueAggregate(
            String type,
            long pendingCount,
            long processingCount,
            long completedCount,
            long failedCount,
            long cancelledCount,
            OffsetDateTime nextRunAt,
            OffsetDateTime lastTerminalAt) {
        private static QueueAggregate empty(String type) {
            return new QueueAggregate(type, 0, 0, 0, 0, 0, null, null);
        }
    }

    private record MetricsRollup(
            double queueP50Ms,
            double queueP95Ms,
            double queueP99Ms,
            double runtimeP50Ms,
            double runtimeP95Ms,
            double runtimeP99Ms,
            long terminalCount,
            long failedCount,
            long completedCount,
            long cancelledCount) {
        private static MetricsRollup empty() {
            return new MetricsRollup(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
    }

    private record RecurringAggregate(
            String type, String cron, OffsetDateTime nextRunAt, OffsetDateTime lastOutcomeAt, long activeCount) {
        private static RecurringAggregate empty(String type, String cron) {
            return new RecurringAggregate(type, cron, null, null, 0);
        }
    }
}
