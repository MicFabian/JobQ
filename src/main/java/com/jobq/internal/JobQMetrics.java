package com.jobq.internal;

import com.jobq.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;

public class JobQMetrics {

    private static final Logger log = LoggerFactory.getLogger(JobQMetrics.class);
    private final JobRepository jobRepository;
    private final MeterRegistry meterRegistry;

    public JobQMetrics(JobRepository jobRepository, MeterRegistry meterRegistry) {
        this.jobRepository = jobRepository;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerMetrics() {
        log.info("Micrometer found on classpath. Registering JobQ gauges...");

        Gauge.builder("jobq.jobs.count", jobRepository, repo -> safeCount(repo::countPendingJobs, "PENDING"))
                .description("Number of JobQ jobs")
                .tag("status", "PENDING")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", jobRepository, repo -> safeCount(repo::countProcessingJobs, "PROCESSING"))
                .description("Number of JobQ jobs")
                .tag("status", "PROCESSING")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", jobRepository, repo -> safeCount(repo::countCompletedJobs, "COMPLETED"))
                .description("Number of JobQ jobs")
                .tag("status", "COMPLETED")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.count", jobRepository, repo -> safeCount(repo::countFailedJobs, "FAILED"))
                .description("Number of JobQ jobs")
                .tag("status", "FAILED")
                .register(meterRegistry);

        Gauge.builder("jobq.jobs.total", jobRepository, JobRepository::count)
                .description("Total number of JobQ jobs in the database")
                .register(meterRegistry);
    }

    private double safeCount(java.util.function.LongSupplier counter, String stateLabel) {
        try {
            return counter.getAsLong();
        } catch (Exception e) {
            log.trace("Failed to query job count for state {}: {}", stateLabel, e.getMessage());
            return 0;
        }
    }
}
