package com.jobq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.config.JobQProperties;
import com.jobq.internal.JobSignalPublisher;
import com.jobq.internal.JobTypeMetadataRegistry;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.ClassUtils;

@Service
public class JobClient {

    private static final Logger log = LoggerFactory.getLogger(JobClient.class);
    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");

    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private final JobQProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final String jobTableName;
    private final String dedupUpsertSql;
    private final String batchInsertSql;
    private final String synchronizeGroupDelaySql;
    private final JobTypeMetadataRegistry jobTypeMetadataRegistry;
    private final JobSignalPublisher jobSignalPublisher;

    @Autowired
    public JobClient(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            JobQProperties properties,
            JdbcTemplate jdbcTemplate,
            JobTypeMetadataRegistry jobTypeMetadataRegistry,
            JobSignalPublisher jobSignalPublisher) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.jobTypeMetadataRegistry = jobTypeMetadataRegistry;
        this.jobSignalPublisher = jobSignalPublisher;
        this.jobTableName = resolveJobTableName(properties.getDatabase().getTablePrefix());
        this.dedupUpsertSql = buildDedupUpsertSql(jobTableName);
        this.batchInsertSql = buildBatchInsertSql(jobTableName);
        this.synchronizeGroupDelaySql = buildSynchronizeGroupDelaySql(jobTableName);
    }

    JobClient(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            JobQProperties properties,
            JdbcTemplate jdbcTemplate,
            JobTypeMetadataRegistry jobTypeMetadataRegistry) {
        this(jobRepository, objectMapper, properties, jdbcTemplate, jobTypeMetadataRegistry, null);
    }

    JobClient(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            JobQProperties properties,
            JdbcTemplate jdbcTemplate) {
        this(jobRepository, objectMapper, properties, jdbcTemplate, null, null);
    }

    /**
     * Enqueue a job with the default number of retries.
     */
    public UUID enqueue(String type, Object payload) {
        return enqueue(type, payload, resolveDefaultMaxRetries(type), null, null, null);
    }

    /**
     * Enqueue a job by class using the class' @Job value.
     */
    public UUID enqueue(Class<?> jobClass, Object payload) {
        return enqueue(resolveJobType(jobClass), payload);
    }

    /**
     * Enqueue a job with a custom number of retries.
     */
    public UUID enqueue(String type, Object payload, int maxRetries) {
        return enqueue(type, payload, maxRetries, null, null, null);
    }

    /**
     * Enqueue a job by class with a custom number of retries.
     */
    public UUID enqueue(Class<?> jobClass, Object payload, int maxRetries) {
        return enqueue(resolveJobType(jobClass), payload, maxRetries);
    }

    /**
     * Enqueue a job with a groupId for parallel group execution.
     */
    public UUID enqueue(String type, Object payload, String groupId) {
        return enqueue(type, payload, resolveDefaultMaxRetries(type), groupId, null, null);
    }

    /**
     * Enqueue a job by class with a groupId for parallel group execution.
     */
    public UUID enqueue(Class<?> jobClass, Object payload, String groupId) {
        return enqueue(resolveJobType(jobClass), payload, groupId);
    }

    /**
     * Enqueue a job with groupId and replaceKey.
     * If a pending job with the same type + replaceKey already exists, JobQ
     * deduplicates and returns that existing job ID.
     */
    public UUID enqueue(String type, Object payload, String groupId, String replaceKey) {
        return enqueue(type, payload, resolveDefaultMaxRetries(type), groupId, replaceKey, null);
    }

    /**
     * Enqueue a job by class with groupId and replaceKey.
     */
    public UUID enqueue(Class<?> jobClass, Object payload, String groupId, String replaceKey) {
        return enqueue(resolveJobType(jobClass), payload, groupId, replaceKey);
    }

    /**
     * Enqueue multiple jobs of the same type efficiently using JDBC batching.
     */
    public List<UUID> enqueueAll(String type, Collection<?> payloads) {
        return enqueueAll(type, payloads, resolveDefaultMaxRetries(type), null);
    }

    /**
     * Enqueue multiple jobs by class efficiently using JDBC batching.
     */
    public List<UUID> enqueueAll(Class<?> jobClass, Collection<?> payloads) {
        return enqueueAll(resolveJobType(jobClass), payloads);
    }

    /**
     * Enqueue multiple jobs of the same type to run at the provided instant.
     */
    public List<UUID> enqueueAllAt(String type, Collection<?> payloads, Instant runAt) {
        return enqueueAll(type, payloads, resolveDefaultMaxRetries(type), normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue multiple jobs by class to run at the provided instant.
     */
    public List<UUID> enqueueAllAt(Class<?> jobClass, Collection<?> payloads, Instant runAt) {
        String jobType = resolveJobType(jobClass);
        return enqueueAll(jobType, payloads, resolveDefaultMaxRetries(jobType), normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue multiple jobs of the same type to run at the provided date-time.
     */
    public List<UUID> enqueueAllAt(String type, Collection<?> payloads, OffsetDateTime runAt) {
        return enqueueAll(type, payloads, resolveDefaultMaxRetries(type), normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue multiple jobs by class to run at the provided date-time.
     */
    public List<UUID> enqueueAllAt(Class<?> jobClass, Collection<?> payloads, OffsetDateTime runAt) {
        String jobType = resolveJobType(jobClass);
        return enqueueAll(jobType, payloads, resolveDefaultMaxRetries(jobType), normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue a job to run at the provided instant.
     */
    public UUID enqueueAt(String type, Object payload, Instant runAt) {
        return enqueueAt(type, payload, resolveDefaultMaxRetries(type), null, null, normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue a job by class to run at the provided instant.
     */
    public UUID enqueueAt(Class<?> jobClass, Object payload, Instant runAt) {
        return enqueueAt(resolveJobType(jobClass), payload, runAt);
    }

    /**
     * Enqueue a job to run at the provided date-time.
     */
    public UUID enqueueAt(String type, Object payload, OffsetDateTime runAt) {
        return enqueueAt(type, payload, resolveDefaultMaxRetries(type), null, null, normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue a job by class to run at the provided date-time.
     */
    public UUID enqueueAt(Class<?> jobClass, Object payload, OffsetDateTime runAt) {
        return enqueueAt(resolveJobType(jobClass), payload, runAt);
    }

    /**
     * Full enqueue-at method with all options.
     */
    public UUID enqueueAt(
            String type, Object payload, int maxRetries, String groupId, String replaceKey, Instant runAt) {
        return enqueueAt(type, payload, maxRetries, groupId, replaceKey, normalizeRequiredRunAt(runAt));
    }

    /**
     * Full enqueue-at method by class with all options.
     */
    public UUID enqueueAt(
            Class<?> jobClass, Object payload, int maxRetries, String groupId, String replaceKey, Instant runAt) {
        return enqueueAt(resolveJobType(jobClass), payload, maxRetries, groupId, replaceKey, runAt);
    }

    /**
     * Full enqueue-at method with all options.
     */
    public UUID enqueueAt(
            String type, Object payload, int maxRetries, String groupId, String replaceKey, OffsetDateTime runAt) {
        return enqueue(type, payload, maxRetries, groupId, replaceKey, normalizeRequiredRunAt(runAt));
    }

    /**
     * Full enqueue-at method by class with all options.
     */
    public UUID enqueueAt(
            Class<?> jobClass,
            Object payload,
            int maxRetries,
            String groupId,
            String replaceKey,
            OffsetDateTime runAt) {
        return enqueueAt(resolveJobType(jobClass), payload, maxRetries, groupId, replaceKey, runAt);
    }

    /**
     * Full enqueue method with all options.
     */
    public UUID enqueue(String type, Object payload, int maxRetries, String groupId, String replaceKey) {
        return enqueue(type, payload, maxRetries, groupId, replaceKey, null);
    }

    /**
     * Full enqueue method by class with all options.
     */
    public UUID enqueue(Class<?> jobClass, Object payload, int maxRetries, String groupId, String replaceKey) {
        return enqueue(resolveJobType(jobClass), payload, maxRetries, groupId, replaceKey);
    }

    private UUID enqueue(
            String type,
            Object payload,
            int maxRetries,
            String groupId,
            String replaceKey,
            OffsetDateTime explicitRunAt) {
        String normalizedType = normalizeRequiredType(type);
        validateRegisteredTypeIfKnown(normalizedType);
        validateMaxRetries(maxRetries);
        String normalizedGroupId = normalizeOptionalString(groupId);
        String normalizedReplaceKey = normalizeOptionalString(replaceKey);
        JsonNode jsonNode = payload != null ? objectMapper.valueToTree(payload) : null;
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime resolvedRunAt = resolveRunAt(normalizedType, explicitRunAt, now);

        // Deduplication path is executed atomically at DB level to avoid exception-driven
        // races under high concurrency.
        if (normalizedReplaceKey != null) {
            boolean updateRunAtOnReplace = shouldUpdateRunAtOnReplace(normalizedType, explicitRunAt);
            UUID dedupedId = upsertPendingDeduplicatedJob(
                    normalizedType,
                    jsonNode,
                    maxRetries,
                    normalizedGroupId,
                    normalizedReplaceKey,
                    resolvedRunAt,
                    now,
                    updateRunAtOnReplace);
            synchronizeGroupedDelayIfConfigured(normalizedType, normalizedGroupId, resolvedRunAt, now);
            log.debug(
                    "Dedup-enqueued job {} of type {} with replaceKey '{}'",
                    dedupedId,
                    normalizedType,
                    normalizedReplaceKey);
            notifyIfDue(normalizedType, resolvedRunAt, now);
            return dedupedId;
        }

        // Fast path when deduplication is not requested.
        UUID jobId = UUID.randomUUID();
        Job job = new Job(jobId, normalizedType, jsonNode, maxRetries, 0, normalizedGroupId, normalizedReplaceKey);
        job.setRunAt(resolvedRunAt);
        job.setUpdatedAt(now);
        jobRepository.save(job);
        synchronizeGroupedDelayIfConfigured(normalizedType, normalizedGroupId, resolvedRunAt, now);
        notifyIfDue(normalizedType, resolvedRunAt, now);
        log.debug("Enqueued job {} of type {}", jobId, normalizedType);
        return jobId;
    }

    private List<UUID> enqueueAll(String type, Collection<?> payloads, int maxRetries, OffsetDateTime explicitRunAt) {
        String normalizedType = normalizeRequiredType(type);
        validateRegisteredTypeIfKnown(normalizedType);
        validateMaxRetries(maxRetries);
        if (payloads == null || payloads.isEmpty()) {
            return List.of();
        }

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime resolvedRunAt = resolveRunAt(normalizedType, explicitRunAt, now);
        List<Object> payloadList = new ArrayList<>(payloads.size());
        payloadList.addAll(payloads);
        List<UUID> ids = new ArrayList<>(payloadList.size());

        jdbcTemplate.batchUpdate(
                batchInsertSql,
                payloadList,
                Math.min(payloadList.size(), 1_000),
                (PreparedStatement preparedStatement, Object payload) -> bindBatchInsert(
                        preparedStatement, ids, normalizedType, payload, resolvedRunAt, now, maxRetries));

        notifyIfDue(normalizedType, resolvedRunAt, now);
        log.debug("Batch-enqueued {} jobs of type {}", ids.size(), normalizedType);
        return List.copyOf(ids);
    }

    private String normalizeRequiredType(String type) {
        if (type == null) {
            throw new IllegalArgumentException("Job type must not be null");
        }
        String trimmed = type.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Job type must not be blank");
        }
        return trimmed;
    }

    private void validateRegisteredTypeIfKnown(String normalizedType) {
        if (jobTypeMetadataRegistry == null) {
            return;
        }
        Set<String> registeredTypes = jobTypeMetadataRegistry.registeredJobTypes();
        if (registeredTypes == null || registeredTypes.isEmpty() || registeredTypes.contains(normalizedType)) {
            return;
        }

        String knownTypesHint = registeredTypes.size() <= 8
                ? " Registered job types: " + String.join(", ", registeredTypes) + "."
                : " Registered job type count: " + registeredTypes.size() + ".";
        throw new IllegalArgumentException("Unknown JobQ type '" + normalizedType
                + "'. No registered JobQ handler matches it. If @Job.value was omitted, JobQ uses job class name as"
                + " type. Use enqueue(MyJob.class, payload) or add explicit @Job(\"" + normalizedType + "\")."
                + knownTypesHint);
    }

    private void validateMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
    }

    private String normalizeOptionalString(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private OffsetDateTime normalizeRequiredRunAt(Instant runAt) {
        if (runAt == null) {
            throw new IllegalArgumentException("runAt must not be null");
        }
        return OffsetDateTime.ofInstant(runAt, ZoneOffset.UTC);
    }

    private OffsetDateTime normalizeRequiredRunAt(OffsetDateTime runAt) {
        if (runAt == null) {
            throw new IllegalArgumentException("runAt must not be null");
        }
        return runAt;
    }

    private OffsetDateTime resolveRunAt(String jobType, OffsetDateTime explicitRunAt, OffsetDateTime now) {
        if (explicitRunAt != null) {
            return explicitRunAt;
        }
        long initialDelayMs = resolveInitialDelayMs(jobType);
        return initialDelayMs <= 0 ? now : now.plus(Duration.ofMillis(initialDelayMs));
    }

    private long resolveInitialDelayMs(String jobType) {
        if (jobTypeMetadataRegistry == null) {
            return 0L;
        }
        return jobTypeMetadataRegistry.initialDelayMsFor(jobType);
    }

    private int resolveDefaultMaxRetries(String jobType) {
        int fallbackMaxRetries = properties.getJobs().getDefaultNumberOfRetries();
        if (jobTypeMetadataRegistry == null) {
            return fallbackMaxRetries;
        }
        String normalizedType = normalizeRequiredType(jobType);
        return jobTypeMetadataRegistry.defaultMaxRetriesFor(normalizedType, fallbackMaxRetries);
    }

    private String resolveJobType(Class<?> jobClass) {
        if (jobTypeMetadataRegistry != null) {
            return normalizeRequiredType(jobTypeMetadataRegistry.jobTypeFor(jobClass));
        }
        if (jobClass == null) {
            throw new IllegalArgumentException("Job class must not be null");
        }
        Class<?> targetClass = ClassUtils.getUserClass(jobClass);
        com.jobq.annotation.Job annotation = AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Job class " + targetClass.getName()
                    + " has no @Job annotation and no registry mapping. Use enqueue(String, payload).");
        }
        String configuredType =
                annotation.value() == null ? "" : annotation.value().trim();
        return normalizeRequiredType(configuredType.isEmpty() ? targetClass.getName() : configuredType);
    }

    private boolean shouldUpdateRunAtOnReplace(String jobType, OffsetDateTime explicitRunAt) {
        if (explicitRunAt != null) {
            return true;
        }
        if (jobTypeMetadataRegistry == null) {
            return true;
        }
        return jobTypeMetadataRegistry.deduplicationRunAtPolicyFor(jobType)
                == com.jobq.annotation.Job.DeduplicationRunAtPolicy.UPDATE_ON_REPLACE;
    }

    private boolean shouldSynchronizeGroupedDelayOnEnqueue(String jobType) {
        if (jobTypeMetadataRegistry == null) {
            return false;
        }
        com.jobq.annotation.Job.GroupDelayPolicy policy = jobTypeMetadataRegistry.groupDelayPolicyFor(jobType);
        return policy == com.jobq.annotation.Job.GroupDelayPolicy.SYNC_WITH_NEW_DELAY;
    }

    private void synchronizeGroupedDelayIfConfigured(
            String type, String groupId, OffsetDateTime runAt, OffsetDateTime now) {
        if (groupId == null || !shouldSynchronizeGroupedDelayOnEnqueue(type)) {
            return;
        }
        int updated = jdbcTemplate.update(synchronizeGroupDelaySql, ps -> {
            ps.setObject(1, runAt);
            ps.setObject(2, now);
            ps.setString(3, type);
            ps.setString(4, groupId);
            ps.setObject(5, runAt);
        });
        if (updated > 0) {
            log.debug("Synchronized runAt for {} pending grouped jobs of type {} in group {}", updated, type, groupId);
        }
    }

    private UUID upsertPendingDeduplicatedJob(
            String type,
            JsonNode payload,
            int maxRetries,
            String groupId,
            String replaceKey,
            OffsetDateTime runAt,
            OffsetDateTime now,
            boolean updateRunAtOnReplace) {
        UUID insertedId = UUID.randomUUID();
        String payloadJson = payload == null ? null : payload.toString();

        UUID resultingId = jdbcTemplate.query(
                dedupUpsertSql,
                ps -> {
                    ps.setObject(1, insertedId);
                    ps.setString(2, type);
                    ps.setString(3, payloadJson);
                    ps.setObject(4, runAt);
                    ps.setObject(5, now);
                    ps.setInt(6, maxRetries);
                    ps.setString(7, groupId);
                    ps.setString(8, replaceKey);
                    ps.setBoolean(9, updateRunAtOnReplace);
                },
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null);

        if (resultingId == null) {
            throw new IllegalStateException("Dedup upsert returned no job id for type " + type);
        }
        return resultingId;
    }

    private void bindBatchInsert(
            PreparedStatement preparedStatement,
            List<UUID> ids,
            String type,
            Object payload,
            OffsetDateTime runAt,
            OffsetDateTime now,
            int maxRetries)
            throws SQLException {
        UUID jobId = UUID.randomUUID();
        ids.add(jobId);
        JsonNode jsonNode = payload != null ? objectMapper.valueToTree(payload) : null;
        preparedStatement.setObject(1, jobId);
        preparedStatement.setString(2, type);
        preparedStatement.setString(3, jsonNode == null ? null : jsonNode.toString());
        preparedStatement.setObject(4, runAt);
        preparedStatement.setObject(5, now);
        preparedStatement.setInt(6, maxRetries);
    }

    private void notifyIfDue(String jobType, OffsetDateTime runAt, OffsetDateTime now) {
        if (jobSignalPublisher != null) {
            jobSignalPublisher.notifyIfDue(jobType, runAt, now);
        }
    }

    private String resolveJobTableName(String tablePrefix) {
        String prefix = tablePrefix == null ? "" : tablePrefix.trim();
        String tableName = prefix + "jobq_jobs";
        if (!SAFE_TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Unsupported job table name: " + tableName);
        }
        return tableName;
    }

    private String buildDedupUpsertSql(String tableName) {
        return """
                INSERT INTO %1$s (id, type, payload, run_at, updated_at, max_retries, priority, group_id, replace_key)
                VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?, 0, ?, ?)
                ON CONFLICT (type, replace_key)
                WHERE processing_started_at IS NULL
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND replace_key IS NOT NULL
                DO UPDATE SET
                  payload = EXCLUDED.payload,
                  run_at = CASE WHEN ? THEN EXCLUDED.run_at ELSE %1$s.run_at END,
                  updated_at = EXCLUDED.updated_at,
                  max_retries = EXCLUDED.max_retries,
                  group_id = EXCLUDED.group_id,
                  processing_started_at = NULL,
                  finished_at = NULL,
                  failed_at = NULL,
                  error_message = NULL,
                  retry_count = 0,
                  locked_at = NULL,
                  locked_by = NULL
                RETURNING id
                """
                .formatted(tableName);
    }

    private String buildBatchInsertSql(String tableName) {
        return """
                INSERT INTO %1$s (id, type, payload, run_at, updated_at, max_retries, priority)
                VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?, 0)
                """
                .formatted(tableName);
    }

    private String buildSynchronizeGroupDelaySql(String tableName) {
        return """
                UPDATE %1$s
                SET run_at = ?,
                    updated_at = ?
                WHERE type = ?
                  AND group_id = ?
                  AND processing_started_at IS NULL
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND (run_at IS NULL OR run_at <> ?)
                """
                .formatted(tableName);
    }
}
