package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.jobq.config.JobQProperties;

import java.time.OffsetDateTime;
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

    public JobClient(JobRepository jobRepository, ObjectMapper objectMapper, JobQProperties properties,
            JdbcTemplate jdbcTemplate) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.jobTableName = resolveJobTableName(properties.getDatabase().getTablePrefix());
        this.dedupUpsertSql = buildDedupUpsertSql(jobTableName);
    }

    /**
     * Enqueue a job with the default number of retries.
     */
    public UUID enqueue(String type, Object payload) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), null, null);
    }

    /**
     * Enqueue a job with a custom number of retries.
     */
    public UUID enqueue(String type, Object payload, int maxRetries) {
        return enqueue(type, payload, maxRetries, null, null);
    }

    /**
     * Enqueue a job with a groupId for parallel group execution.
     */
    public UUID enqueue(String type, Object payload, String groupId) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), groupId, null);
    }

    /**
     * Enqueue a job with groupId and replaceKey.
     * If a PENDING job with the same type + replaceKey already exists,
     * its payload is updated in-place and the existing job ID is returned.
     */
    public UUID enqueue(String type, Object payload, String groupId, String replaceKey) {
        return enqueue(type, payload, properties.getJobs().getDefaultNumberOfRetries(), groupId, replaceKey);
    }

    /**
     * Full enqueue method with all options.
     */
    public UUID enqueue(String type, Object payload, int maxRetries, String groupId, String replaceKey) {
        String normalizedType = normalizeRequiredType(type);
        validateMaxRetries(maxRetries);
        String normalizedGroupId = normalizeOptionalString(groupId);
        String normalizedReplaceKey = normalizeOptionalString(replaceKey);
        JsonNode jsonNode = payload != null ? objectMapper.valueToTree(payload) : null;

        // Deduplication path is executed atomically at DB level to avoid exception-driven
        // races under high concurrency.
        if (normalizedReplaceKey != null) {
            UUID dedupedId = upsertPendingDeduplicatedJob(normalizedType, jsonNode, maxRetries, normalizedGroupId,
                    normalizedReplaceKey);
            log.debug("Dedup-enqueued job {} of type {} with replaceKey '{}'", dedupedId, normalizedType,
                    normalizedReplaceKey);
            return dedupedId;
        }

        // Fast path when deduplication is not requested.
        UUID jobId = UUID.randomUUID();
        Job job = new Job(jobId, normalizedType, jsonNode, maxRetries, 0, normalizedGroupId, normalizedReplaceKey);
        job.setUpdatedAt(OffsetDateTime.now());
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

    private UUID upsertPendingDeduplicatedJob(String type, JsonNode payload, int maxRetries, String groupId,
            String replaceKey) {
        UUID insertedId = UUID.randomUUID();
        String payloadJson = payload == null ? null : payload.toString();
        OffsetDateTime now = OffsetDateTime.now();

        UUID resultingId = jdbcTemplate.queryForObject(
                dedupUpsertSql,
                (rs, rowNum) -> rs.getObject("id", UUID.class),
                insertedId, type, payloadJson, now, maxRetries, groupId, replaceKey);

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
                INSERT INTO %s (id, type, payload, updated_at, max_retries, priority, group_id, replace_key)
                VALUES (?, ?, CAST(? AS jsonb), ?, ?, 0, ?, ?)
                ON CONFLICT (type, replace_key)
                WHERE processing_started_at IS NULL
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND replace_key IS NOT NULL
                DO UPDATE SET
                  payload = EXCLUDED.payload,
                  updated_at = EXCLUDED.updated_at,
                  max_retries = EXCLUDED.max_retries,
                  group_id = EXCLUDED.group_id,
                  processing_started_at = NULL,
                  finished_at = NULL,
                  failed_at = NULL,
                  error_message = NULL,
                  retry_count = 0
                RETURNING id
                """.formatted(tableName);
    }
}
