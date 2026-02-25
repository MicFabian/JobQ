package com.jobq;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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

    @Column(nullable = false)
    private String status = "PENDING";

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @Column(name = "locked_at")
    private OffsetDateTime lockedAt;

    @Column(name = "locked_by")
    private String lockedBy;

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

    public Job() {
        this.runAt = OffsetDateTime.now();
    }

    public Job(UUID id, String type, com.fasterxml.jackson.databind.JsonNode payload, int maxRetries, int priority) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.status = "PENDING";
        this.maxRetries = maxRetries;
        this.priority = priority;
        this.runAt = OffsetDateTime.now();
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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
}
