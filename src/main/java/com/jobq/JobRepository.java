package com.jobq;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {

    /**
     * Aggregated lifecycle counters fetched in a single query.
     */
    interface LifecycleCounts {
        Long getPendingCount();

        Long getProcessingCount();

        Long getCompletedCount();

        Long getFailedCount();
    }

    /**
     * Lightweight projection used by the dashboard list to avoid loading large payload JSON.
     */
    interface DashboardJobView {
        UUID getId();

        String getType();

        OffsetDateTime getCreatedAt();

        OffsetDateTime getRunAt();

        int getRetryCount();

        int getMaxRetries();

        int getPriority();

        String getGroupId();

        String getReplaceKey();

        OffsetDateTime getProcessingStartedAt();

        OffsetDateTime getFinishedAt();

        OffsetDateTime getFailedAt();

        String getErrorMessage();
    }

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            WHERE (
              (:status = 'PENDING' AND j.processingStartedAt IS NULL AND j.finishedAt IS NULL AND j.failedAt IS NULL)
              OR (:status = 'PROCESSING' AND j.processingStartedAt IS NOT NULL AND j.finishedAt IS NULL AND j.failedAt IS NULL)
              OR (:status = 'COMPLETED' AND j.finishedAt IS NOT NULL)
              OR (:status = 'FAILED' AND j.failedAt IS NOT NULL)
              OR (:status = '')
            )
              AND (:scheduledOnly = false OR j.runAt > CURRENT_TIMESTAMP)
              AND (:retriedOnly = false OR j.retryCount > 0)
              AND (
                :query = ''
                OR LOWER(j.type) LIKE LOWER(CONCAT('%', :query, '%'))
                OR LOWER(COALESCE(j.groupId, '')) LIKE LOWER(CONCAT('%', :query, '%'))
                OR LOWER(COALESCE(j.replaceKey, '')) LIKE LOWER(CONCAT('%', :query, '%'))
                OR (:jobIdProvided = true AND j.id = :jobId)
              )
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findDashboardJobViews(
            @Param("status") String status,
            @Param("query") String query,
            @Param("scheduledOnly") boolean scheduledOnly,
            @Param("retriedOnly") boolean retriedOnly,
            @Param("jobIdProvided") boolean jobIdProvided,
            @Param("jobId") UUID jobId,
            Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")}) // SKIP LOCKED
    @Query(
            """
            SELECT j FROM Job j
            WHERE j.type = :type
              AND j.processingStartedAt IS NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
              AND j.runAt <= CURRENT_TIMESTAMP
            ORDER BY j.priority DESC, j.createdAt ASC
            """)
    List<Job> findNextJobsForUpdate(@Param("type") String type, Pageable pageable);

    @Query(
            """
            SELECT j FROM Job j
            WHERE j.processingStartedAt IS NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<Job> findPendingJobs(Pageable pageable);

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            WHERE j.processingStartedAt IS NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findPendingJobViews(Pageable pageable);

    @Query(
            """
            SELECT j FROM Job j
            WHERE j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<Job> findProcessingJobs(Pageable pageable);

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            WHERE j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findProcessingJobViews(Pageable pageable);

    @Query("SELECT j FROM Job j WHERE j.finishedAt IS NOT NULL")
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<Job> findCompletedJobs(Pageable pageable);

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            WHERE j.finishedAt IS NOT NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findCompletedJobViews(Pageable pageable);

    @Query("SELECT j FROM Job j WHERE j.failedAt IS NOT NULL")
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<Job> findFailedJobs(Pageable pageable);

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            WHERE j.failedAt IS NOT NULL
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findFailedJobViews(Pageable pageable);

    @Query("SELECT j FROM Job j")
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<Job> findAllJobs(Pageable pageable);

    @Query(
            """
            SELECT
              j.id AS id,
              j.type AS type,
              j.createdAt AS createdAt,
              j.runAt AS runAt,
              j.retryCount AS retryCount,
              j.maxRetries AS maxRetries,
              j.priority AS priority,
              j.groupId AS groupId,
              j.replaceKey AS replaceKey,
              j.processingStartedAt AS processingStartedAt,
              j.finishedAt AS finishedAt,
              j.failedAt AS failedAt,
              j.errorMessage AS errorMessage
            FROM Job j
            """)
    @QueryHints(@QueryHint(name = "org.hibernate.readOnly", value = "true"))
    Slice<DashboardJobView> findAllJobViews(Pageable pageable);

    @Query(
            """
            SELECT COUNT(j) FROM Job j
            WHERE j.processingStartedAt IS NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    long countPendingJobs();

    @Query(
            """
            SELECT COUNT(j) FROM Job j
            WHERE j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
            """)
    long countProcessingJobs();

    @Query("SELECT COUNT(j) FROM Job j WHERE j.finishedAt IS NOT NULL")
    long countCompletedJobs();

    @Query("SELECT COUNT(j) FROM Job j WHERE j.failedAt IS NOT NULL")
    long countFailedJobs();

    @Query(
            """
            SELECT
              COALESCE(SUM(CASE
                WHEN j.processingStartedAt IS NULL AND j.finishedAt IS NULL AND j.failedAt IS NULL
                THEN 1 ELSE 0 END), 0) AS pendingCount,
              COALESCE(SUM(CASE
                WHEN j.processingStartedAt IS NOT NULL AND j.finishedAt IS NULL AND j.failedAt IS NULL
                THEN 1 ELSE 0 END), 0) AS processingCount,
              COALESCE(SUM(CASE
                WHEN j.finishedAt IS NOT NULL
                THEN 1 ELSE 0 END), 0) AS completedCount,
              COALESCE(SUM(CASE
                WHEN j.failedAt IS NOT NULL
                THEN 1 ELSE 0 END), 0) AS failedCount
            FROM Job j
            """)
    LifecycleCounts countLifecycleCounts();

    @Modifying
    @Transactional
    int deleteByFinishedAtBefore(OffsetDateTime finishedAt);

    @Modifying
    @Transactional
    int deleteByFailedAtBefore(OffsetDateTime failedAt);

    @Modifying
    @Query(
            """
            UPDATE Job j
            SET j.processingStartedAt = COALESCE(j.processingStartedAt, :now),
                j.finishedAt = :now,
                j.failedAt = NULL,
                j.errorMessage = NULL,
                j.lockedAt = NULL,
                j.lockedBy = NULL,
                j.updatedAt = :now
            WHERE j.id = :id
              AND j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
              AND j.lockedAt IS NOT NULL
              AND j.lockedBy = :lockedBy
            """)
    int markCompleted(@Param("id") UUID id, @Param("now") OffsetDateTime now, @Param("lockedBy") String lockedBy);

    @Modifying
    @Query(
            """
            UPDATE Job j
            SET j.retryCount = :nextRetryCount,
                j.errorMessage = :errorMessage,
                j.updatedAt = :now,
                j.processingStartedAt = COALESCE(j.processingStartedAt, :now),
                j.failedAt = :now,
                j.finishedAt = NULL,
                j.lockedAt = NULL,
                j.lockedBy = NULL
            WHERE j.id = :id
              AND j.retryCount = :expectedRetryCount
              AND j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
              AND j.lockedAt IS NOT NULL
              AND j.lockedBy = :lockedBy
            """)
    int markFailedTerminal(
            @Param("id") UUID id,
            @Param("expectedRetryCount") int expectedRetryCount,
            @Param("nextRetryCount") int nextRetryCount,
            @Param("errorMessage") String errorMessage,
            @Param("now") OffsetDateTime now,
            @Param("lockedBy") String lockedBy);

    @Modifying
    @Query(
            """
            UPDATE Job j
            SET j.retryCount = :nextRetryCount,
                j.errorMessage = :errorMessage,
                j.updatedAt = :now,
                j.processingStartedAt = NULL,
                j.finishedAt = NULL,
                j.failedAt = NULL,
                j.lockedAt = NULL,
                j.lockedBy = NULL,
                j.runAt = :nextRunAt,
                j.priority = :nextPriority
            WHERE j.id = :id
              AND j.retryCount = :expectedRetryCount
              AND j.processingStartedAt IS NOT NULL
              AND j.finishedAt IS NULL
              AND j.failedAt IS NULL
              AND j.lockedAt IS NOT NULL
              AND j.lockedBy = :lockedBy
            """)
    int markForRetry(
            @Param("id") UUID id,
            @Param("expectedRetryCount") int expectedRetryCount,
            @Param("nextRetryCount") int nextRetryCount,
            @Param("errorMessage") String errorMessage,
            @Param("now") OffsetDateTime now,
            @Param("nextRunAt") OffsetDateTime nextRunAt,
            @Param("nextPriority") int nextPriority,
            @Param("lockedBy") String lockedBy);

    boolean existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(String type, String cron);
}
