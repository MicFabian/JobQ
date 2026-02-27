package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.jobq.internal.JobTypeMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.jobq.config.JobQProperties;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.regex.Pattern;

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
    private final JobTypeMetadataRegistry jobTypeMetadataRegistry;

    @Autowired
    public JobClient(JobRepository jobRepository, ObjectMapper objectMapper, JobQProperties properties,
            JdbcTemplate jdbcTemplate, JobTypeMetadataRegistry jobTypeMetadataRegistry) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.jobTypeMetadataRegistry = jobTypeMetadataRegistry;
        this.jobTableName = resolveJobTableName(properties.getDatabase().getTablePrefix());
        this.dedupUpsertSql = buildDedupUpsertSql(jobTableName);
    }

    JobClient(JobRepository jobRepository, ObjectMapper objectMapper, JobQProperties properties,
            JdbcTemplate jdbcTemplate) {
        this(jobRepository, objectMapper, properties, jdbcTemplate, null);
    }

    /**
     * Enqueue a job with the default number of retries.
     */
    public UUID enqueue(String type, Object payload) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), null, null, null);
    }

    /**
     * Enqueue a job with a custom number of retries.
     */
    public UUID enqueue(String type, Object payload, int maxRetries) {
        return enqueue(type, payload, maxRetries, null, null, null);
    }

    /**
     * Enqueue a job with a groupId for parallel group execution.
     */
    public UUID enqueue(String type, Object payload, String groupId) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), groupId, null, null);
    }

    /**
     * Enqueue a job with groupId and replaceKey.
     * If a pending job with the same type + replaceKey already exists, JobQ
     * deduplicates and returns that existing job ID.
     */
    public UUID enqueue(String type, Object payload, String groupId, String replaceKey) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), groupId, replaceKey, null);
    }

    /**
     * Enqueue a job to run at the provided instant.
     */
    public UUID enqueueAt(String type, Object payload, Instant runAt) {
        return enqueueAt(type, payload, properties.getJobs().getDefaultNumberOfRetries(), null, null,
                normalizeRequiredRunAt(runAt));
    }

    /**
     * Enqueue a job to run at the provided date-time.
     */
    public UUID enqueueAt(String type, Object payload, OffsetDateTime runAt) {
        return enqueueAt(type, payload, properties.getJobs().getDefaultNumberOfRetries(), null, null,
                normalizeRequiredRunAt(runAt));
    }

    /**
     * Full enqueue-at method with all options.
     */
    public UUID enqueueAt(String type, Object payload, int maxRetries, String groupId, String replaceKey,
            Instant runAt) {
        return enqueueAt(type, payload, maxRetries, groupId, replaceKey, normalizeRequiredRunAt(runAt));
    }

    /**
     * Full enqueue-at method with all options.
     */
    public UUID enqueueAt(String type, Object payload, int maxRetries, String groupId, String replaceKey,
            OffsetDateTime runAt) {
        return enqueue(type, payload, maxRetries, groupId, replaceKey, normalizeRequiredRunAt(runAt));
    }

    /**
     * Full enqueue method with all options.
     */
    public UUID enqueue(String type, Object payload, int maxRetries, String groupId, String replaceKey) {
        return enqueue(type, payload, maxRetries, groupId, replaceKey, null);
    }

    private UUID enqueue(String type, Object payload, int maxRetries, String groupId, String replaceKey,
            OffsetDateTime explicitRunAt) {
        String normalizedType = normalizeRequiredType(type);
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
            UUID dedupedId = upsertPendingDeduplicatedJob(normalizedType, jsonNode, maxRetries, normalizedGroupId,
                    normalizedReplaceKey, resolvedRunAt, now, updateRunAtOnReplace);
            log.debug("Dedup-enqueued job {} of type {} with replaceKey '{}'", dedupedId, normalizedType,
                    normalizedReplaceKey);
            return dedupedId;
        }

        // Fast path when deduplication is not requested.
        UUID jobId = UUID.randomUUID();
        Job job = new Job(jobId, normalizedType, jsonNode, maxRetries, 0, normalizedGroupId, normalizedReplaceKey);
        job.setRunAt(resolvedRunAt);
        job.setUpdatedAt(now);
        jobRepository.save(job);
        log.debug("Enqueued job {} of type {}", jobId, normalizedType);
        return jobId;
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

    private boolean shouldUpdateRunAtOnReplace(String jobType, OffsetDateTime explicitRunAt) {
        if (explicitRunAt != null) {
            return true;
        }
        if (jobTypeMetadataRegistry == null) {
            return true;
        }
        return jobTypeMetadataRegistry
                .deduplicationRunAtPolicyFor(jobType) == com.jobq.annotation.Job.DeduplicationRunAtPolicy.UPDATE_ON_REPLACE;
    }

    private UUID upsertPendingDeduplicatedJob(String type, JsonNode payload, int maxRetries, String groupId,
            String replaceKey, OffsetDateTime runAt, OffsetDateTime now, boolean updateRunAtOnReplace) {
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
                """.formatted(tableName);
    }
}
