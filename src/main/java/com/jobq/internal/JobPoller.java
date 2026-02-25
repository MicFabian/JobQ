package com.jobq.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(prefix = "jobq.background-job-server", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobPoller {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);

    private final JobRepository jobRepository;
    private final List<JobWorker<?>> workers;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final com.jobq.config.JobQProperties properties;

    private final ExecutorService executorService;

    private final String nodeId = "node-" + UUID.randomUUID().toString();
    private Map<String, JobWorker<?>> workerMap;

    public JobPoller(JobRepository jobRepository, List<JobWorker<?>> workers,
            ObjectMapper objectMapper, TransactionTemplate transactionTemplate,
            com.jobq.config.JobQProperties properties) {
        this.jobRepository = jobRepository;
        this.workers = workers;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.executorService = Executors.newFixedThreadPool(properties.getBackgroundJobServer().getWorkerCount());
    }

    @PostConstruct
    public void init() {
        workerMap = workers.stream()
                .collect(Collectors.toMap(JobWorker::getJobType, Function.identity()));
        log.info("Job Poller initialized on {} with {} registered workers: {}", nodeId, workers.size(),
                workerMap.keySet());
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.poll-interval-in-seconds:15}000")
    public void poll() {
        if (workerMap.isEmpty()) {
            return;
        }
        for (String jobType : workerMap.keySet()) {
            executorService.submit(() -> pollForType(jobType));
        }
    }

    private void pollForType(String jobType) {
        JobWorker<?> worker = workerMap.get(jobType);
        if (worker == null) {
            return;
        }

        // 1. Acquire and lock next job using a short transaction
        Job job = transactionTemplate.execute(status -> {
            List<Job> nextJobs = jobRepository.findNextJobsForUpdate(jobType, PageRequest.of(0, 1));
            if (nextJobs.isEmpty()) {
                return null;
            }
            Job j = nextJobs.get(0);
            j.setStatus("PROCESSING");
            j.setLockedAt(OffsetDateTime.now());
            j.setLockedBy(nodeId);
            j.setUpdatedAt(OffsetDateTime.now());
            return jobRepository.save(j);
        });

        if (job == null) {
            return; // No work
        }

        log.info("Locked job {} of type {} for processing", job.getId(), job.getType());

        // 2. Process job detached from DB transaction
        try {
            com.fasterxml.jackson.databind.JsonNode rawPayload = job.getPayload();
            Object payload = null;
            if (rawPayload != null) {
                payload = objectMapper.treeToValue(rawPayload, worker.getPayloadClass());
            }

            @SuppressWarnings("unchecked")
            JobWorker<Object> castWorker = (JobWorker<Object>) worker;
            castWorker.process(job.getId(), payload);

            // 3. Mark completed
            transactionTemplate.executeWithoutResult(status -> {
                jobRepository.findById(job.getId()).ifPresent(j -> {
                    j.setStatus("COMPLETED");
                    j.setUpdatedAt(OffsetDateTime.now());
                    jobRepository.save(j);
                });
            });
            log.info("Successfully completed job {} of type {}", job.getId(), job.getType());

        } catch (Exception e) {
            log.error("Failed to process job {} of type {}", job.getId(), job.getType(), e);

            // 4. Mark failed and handle retry using @Job semantics
            transactionTemplate.executeWithoutResult(status -> {
                jobRepository.findById(job.getId()).ifPresent(j -> {
                    j.setErrorMessage(e.getMessage());
                    j.incrementRetryCount();
                    j.setUpdatedAt(OffsetDateTime.now());

                    if (j.getRetryCount() > j.getMaxRetries()) {
                        j.setStatus("FAILED"); // Max retries exceeded
                    } else {
                        j.setStatus("PENDING"); // Reschedule

                        // Parse @Job annotation to compute backoff and priority shifts
                        // using Spring AopUtils/ClassUtils to handle proxies correctly.
                        com.jobq.annotation.Job jobAnnotation = org.springframework.core.annotation.AnnotationUtils
                                .findAnnotation(worker.getClass(), com.jobq.annotation.Job.class);

                        if (jobAnnotation != null) {
                            // Exponential backoff
                            long delayMs = (long) (jobAnnotation.initialBackoffMs()
                                    * Math.pow(jobAnnotation.backoffMultiplier(), j.getRetryCount() - 1));
                            j.setRunAt(OffsetDateTime.now().plusNanos(delayMs * 1_000_000));

                            // Adjust priorities
                            if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY) {
                                j.setPriority(j.getPriority() - 1);
                            } else if (jobAnnotation
                                    .retryPriority() == com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY) {
                                j.setPriority(j.getPriority() + 1);
                            }
                        } else {
                            // Default exponential backoff using configurations
                            long delaySeconds = (long) Math.pow(properties.getJobs().getRetryBackOffTimeSeed(),
                                    Math.max(1, j.getRetryCount()));
                            j.setRunAt(OffsetDateTime.now().plusSeconds(delaySeconds));
                        }
                    }
                    jobRepository.save(j);
                });
            });
        }
    }
}
