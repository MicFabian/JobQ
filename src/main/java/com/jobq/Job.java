package com.jobq;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "jobq_jobs")
public class Job {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private com.fasterxml.jackson.databind.JsonNode payload;

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @Column(name = "locked_at")
    private OffsetDateTime lockedAt;

    @Column(name = "locked_by")
    private String lockedBy;

    @Column(name = "processing_started_at")
    private OffsetDateTime processingStartedAt;

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Column(name = "failed_at")
    private OffsetDateTime failedAt;

    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    @Column(name = "retry_count")
    private int retryCount = 0;

    @Column(name = "max_retries")
    private int maxRetries = 3;

    @Column(name = "priority")
    private int priority = 0;

    @Column(name = "run_at")
    private OffsetDateTime runAt;

    @Column(name = "group_id")
    private String groupId;

    @Column(name = "replace_key")
    private String replaceKey;

    @Column(name = "cron")
    private String cron;

    public Job() {
        this.runAt = OffsetDateTime.now();
    }

    public Job(UUID id, String type, com.fasterxml.jackson.databind.JsonNode payload, int maxRetries, int priority) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.maxRetries = maxRetries;
        this.priority = priority;
        this.runAt = OffsetDateTime.now();
    }

    public Job(UUID id, String type, com.fasterxml.jackson.databind.JsonNode payload, int maxRetries, int priority,
            String groupId, String replaceKey) {
        this(id, type, payload, maxRetries, priority);
        this.groupId = groupId;
        this.replaceKey = replaceKey;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public com.fasterxml.jackson.databind.JsonNode getPayload() {
        return payload;
    }

    public void setPayload(com.fasterxml.jackson.databind.JsonNode payload) {
        this.payload = payload;
    }

    @Transient
    public String getStatus() {
        if (finishedAt != null) {
            return "COMPLETED";
        }
        if (failedAt != null) {
            return "FAILED";
        }
        if (processingStartedAt != null) {
            return "PROCESSING";
        }
        return "PENDING";
    }

    public void setStatus(String status) {
        OffsetDateTime now = OffsetDateTime.now();
        switch (status) {
            case "PENDING" -> {
                this.processingStartedAt = null;
                this.finishedAt = null;
                this.failedAt = null;
            }
            case "PROCESSING" -> {
                if (this.processingStartedAt == null) {
                    this.processingStartedAt = now;
                }
                this.finishedAt = null;
                this.failedAt = null;
            }
            case "COMPLETED" -> {
                this.finishedAt = now;
                this.failedAt = null;
            }
            case "FAILED" -> {
                this.failedAt = now;
                this.finishedAt = null;
            }
            default -> throw new IllegalArgumentException("Unsupported status: " + status);
        }
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public OffsetDateTime getLockedAt() {
        return lockedAt;
    }

    public void setLockedAt(OffsetDateTime lockedAt) {
        this.lockedAt = lockedAt;
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public OffsetDateTime getProcessingStartedAt() {
        return processingStartedAt;
    }

    public void setProcessingStartedAt(OffsetDateTime processingStartedAt) {
        this.processingStartedAt = processingStartedAt;
    }

    public OffsetDateTime getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(OffsetDateTime finishedAt) {
        this.finishedAt = finishedAt;
    }

    public OffsetDateTime getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(OffsetDateTime failedAt) {
        this.failedAt = failedAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public OffsetDateTime getRunAt() {
        return runAt;
    }

    public void setRunAt(OffsetDateTime runAt) {
        this.runAt = runAt;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getReplaceKey() {
        return replaceKey;
    }

    public void setReplaceKey(String replaceKey) {
        this.replaceKey = replaceKey;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
