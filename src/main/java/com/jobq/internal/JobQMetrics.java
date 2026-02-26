package com.jobq.internal;

import com.jobq.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;

import java.time.Duration;

public class JobQMetrics {

    private static final Logger log = LoggerFactory.getLogger(JobQMetrics.class);
    private static final long SNAPSHOT_TTL_NANOS = Duration.ofSeconds(1).toNanos();

    private final JobRepository jobRepository;
    private final MeterRegistry meterRegistry;
    private final Object snapshotMonitor = new Object();

    private volatile LifecycleSnapshot cachedSnapshot = LifecycleSnapshot.empty();
    private volatile long snapshotCapturedAtNanos = 0L;

    public JobQMetrics(JobRepository jobRepository, MeterRegistry meterRegistry) {
        this.jobRepository = jobRepository;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerMetrics() {
        log.info("Micrometer found on classpath. Registering JobQ gauges...");

        Gauge.builder("jobq.jobs.count", this, metrics -> metrics.countFor(Status.PENDING))
                .description("Number of JobQ jobs")
                .tag("status", "PENDING")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", this, metrics -> metrics.countFor(Status.PROCESSING))
                .description("Number of JobQ jobs")
                .tag("status", "PROCESSING")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", this, metrics -> metrics.countFor(Status.COMPLETED))
                .description("Number of JobQ jobs")
                .tag("status", "COMPLETED")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", this, metrics -> metrics.countFor(Status.FAILED))
                .description("Number of JobQ jobs")
                .tag("status", "FAILED")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.total", this, JobQMetrics::totalCount)
                .description("Total number of JobQ jobs in the database")
                .register(meterRegistry);
    }

    private double countFor(Status status) {
        LifecycleSnapshot snapshot = getSnapshot();
        return switch (status) {
            case PENDING -> snapshot.pendingCount();
            case PROCESSING -> snapshot.processingCount();
            case COMPLETED -> snapshot.completedCount();
            case FAILED -> snapshot.failedCount();
        };
    }

    private double totalCount() {
        LifecycleSnapshot snapshot = getSnapshot();
        return snapshot.pendingCount() + snapshot.processingCount() + snapshot.completedCount() + snapshot.failedCount();
    }

    private LifecycleSnapshot getSnapshot() {
        long now = System.nanoTime();
        LifecycleSnapshot currentSnapshot = cachedSnapshot;
        if (now - snapshotCapturedAtNanos <= SNAPSHOT_TTL_NANOS) {
            return currentSnapshot;
        }

        synchronized (snapshotMonitor) {
            now = System.nanoTime();
            if (now - snapshotCapturedAtNanos <= SNAPSHOT_TTL_NANOS) {
                return cachedSnapshot;
            }
            cachedSnapshot = loadSnapshot();
            snapshotCapturedAtNanos = now;
            return cachedSnapshot;
        }
    }

    private LifecycleSnapshot loadSnapshot() {
        try {
            JobRepository.LifecycleCounts counts = jobRepository.countLifecycleCounts();
            return new LifecycleSnapshot(
                    countOrZero(counts.getPendingCount()),
                    countOrZero(counts.getProcessingCount()),
                    countOrZero(counts.getCompletedCount()),
                    countOrZero(counts.getFailedCount()));
        } catch (Exception e) {
            log.trace("Failed to query lifecycle counts for metrics: {}", e.getMessage());
            return LifecycleSnapshot.empty();
        }
    }

    private long countOrZero(Long value) {
        return value == null ? 0L : value;
    }

    private enum Status {
        PENDING,
        PROCESSING,
        COMPLETED,
        FAILED
    }

    private record LifecycleSnapshot(
            long pendingCount,
            long processingCount,
            long completedCount,
            long failedCount) {
        private static LifecycleSnapshot empty() {
            return new LifecycleSnapshot(0, 0, 0, 0);
        }
    }
}
