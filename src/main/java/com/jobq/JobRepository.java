package com.jobq;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;
import java.time.OffsetDateTime;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({ @QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2") }) // SKIP LOCKED
    @Query("SELECT j FROM Job j WHERE j.type = :type AND j.status = 'PENDING' AND j.runAt <= CURRENT_TIMESTAMP ORDER BY j.priority DESC, j.createdAt ASC")
    List<Job> findNextJobsForUpdate(@Param("type") String type, Pageable pageable);

    Page<Job> findByStatus(String status, Pageable pageable);

    long countByStatus(String status);

    int deleteByStatusAndUpdatedAtBefore(String status, OffsetDateTime updatedAt);
}
