package com.jobq.internal;

import com.jobq.JobRepository;
import com.jobq.config.JobQProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Locale;

@Component
@ConditionalOnProperty(prefix = "jobq.background-job-server", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobCleaner {

    private static final Logger log = LoggerFactory.getLogger(JobCleaner.class);
    private final JobRepository jobRepository;
    private final JobQProperties properties;

    public JobCleaner(JobRepository jobRepository, JobQProperties properties) {
        this.jobRepository = jobRepository;
        this.properties = properties;
    }

    // Run cleaner every hour
    @Scheduled(fixedDelay = 3600000)
    public void cleanup() {
        log.info("Running JobQ automatic cleanup task...");

        try {
            String successRetentionStr = properties.getBackgroundJobServer().getDeleteSucceededJobsAfter();
            if (successRetentionStr != null && !successRetentionStr.isEmpty()) {
                Duration retention = parseDuration(successRetentionStr);
                OffsetDateTime threshold = OffsetDateTime.now().minus(retention);
                int deletedSuccess = jobRepository.deleteByFinishedAtBefore(threshold);
                if (deletedSuccess > 0) {
                    log.info("Cleaned up {} successfully completed jobs older than {}", deletedSuccess, retention);
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up succeeded jobs: {}", e.getMessage());
        }

        try {
            String failedRetentionStr = properties.getBackgroundJobServer().getPermanentlyDeleteDeletedJobsAfter();
            if (failedRetentionStr != null && !failedRetentionStr.isEmpty()) {
                Duration retention = parseDuration(failedRetentionStr);
                OffsetDateTime threshold = OffsetDateTime.now().minus(retention);
                int deletedFailed = jobRepository.deleteByFailedAtBefore(threshold);
                if (deletedFailed > 0) {
                    log.info("Permanently deleted {} failed jobs older than {}", deletedFailed, retention);
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean up failed jobs: {}", e.getMessage());
        }
    }

    private Duration parseDuration(String durationStr) {
        String trimmed = durationStr.trim();
        try {
            return Duration.parse(trimmed);
        } catch (RuntimeException ignored) {
            // Continue with shorthand parsing below.
        }

        // Supports shorthand inputs like "36h" or "7d".
        String shorthand = trimmed.toLowerCase(Locale.ROOT);
        if (shorthand.endsWith("h")) {
            long hours = Long.parseLong(shorthand.substring(0, shorthand.length() - 1));
            return Duration.ofHours(hours);
        } else if (shorthand.endsWith("d")) {
            long days = Long.parseLong(shorthand.substring(0, shorthand.length() - 1));
            return Duration.ofDays(days);
        }
        throw new IllegalArgumentException("Unsupported duration value: " + durationStr);
    }
}
