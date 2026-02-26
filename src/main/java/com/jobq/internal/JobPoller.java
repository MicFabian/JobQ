package com.jobq.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final int workerCount;
    private final ThreadPoolExecutor processingExecutor;
    private final ThreadPoolExecutor pollingExecutor;

    private final String nodeId = "node-" + UUID.randomUUID().toString();
    private Map<String, JobWorker<?>> workerMap;
    private Map<JobWorker<?>, com.jobq.annotation.Job> workerAnnotationCache = Map.of();
    private Map<String, AtomicBoolean> pollInProgress = Map.of();

    public JobPoller(JobRepository jobRepository, List<JobWorker<?>> workers,
            ObjectMapper objectMapper, TransactionTemplate transactionTemplate,
            com.jobq.config.JobQProperties properties) {
        this.jobRepository = jobRepository;
        this.workers = workers;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.workerCount = Math.max(1, properties.getBackgroundJobServer().getWorkerCount());

        int processingQueueCapacity = Math.max(32, workerCount * 8);
        this.processingExecutor = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(processingQueueCapacity),
                new ThreadPoolExecutor.CallerRunsPolicy());

        int pollThreads = Math.min(4, Math.max(1, workerCount / 2));
        int pollingQueueCapacity = Math.max(32, workerCount * 4);
        this.pollingExecutor = new ThreadPoolExecutor(
                pollThreads,
                pollThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(pollingQueueCapacity),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @PostConstruct
    public void init() {
        workerMap = workers.stream()
                .collect(Collectors.toMap(JobWorker::getJobType, Function.identity()));
        Map<JobWorker<?>, com.jobq.annotation.Job> annotations = new IdentityHashMap<>();
        for (JobWorker<?> worker : workers) {
            annotations.put(worker, findJobAnnotationOnClass(worker));
        }
        workerAnnotationCache = annotations;

        Map<String, AtomicBoolean> pollState = new HashMap<>();
        for (String jobType : workerMap.keySet()) {
            pollState.put(jobType, new AtomicBoolean(false));
        }
        pollInProgress = pollState;

        log.info("Job Poller initialized on {} with {} registered workers: {}", nodeId, workers.size(),
                workerMap.keySet());
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.poll-interval-in-seconds:15}000")
    public void poll() {
        if (workerMap.isEmpty()) {
            return;
        }
        for (String jobType : workerMap.keySet()) {
            AtomicBoolean inProgress = pollInProgress.get(jobType);
            if (inProgress == null || !inProgress.compareAndSet(false, true)) {
                continue;
            }

            try {
                pollingExecutor.execute(() -> {
                    try {
                        pollForType(jobType);
                    } finally {
                        inProgress.set(false);
                    }
                });
            } catch (RejectedExecutionException saturatedPollingQueue) {
                inProgress.set(false);
                log.debug("Skipping poll dispatch for type {} because polling queue is saturated", jobType);
            }
        }
    }

    private void pollForType(String jobType) {
        JobWorker<?> worker = workerMap.get(jobType);
        if (worker == null) {
            return;
        }

        int availableSlots = availableProcessingSlots();
        if (availableSlots <= 0) {
            return;
        }

        // Acquire a bounded batch based on currently available processing capacity.
        List<Job> jobs = transactionTemplate.execute(status -> {
            int batchSize = Math.min(workerCount, availableSlots);
            List<Job> nextJobs = jobRepository.findNextJobsForUpdate(jobType, PageRequest.of(0, batchSize));
            if (nextJobs.isEmpty()) {
                return List.<Job>of();
            }
            OffsetDateTime lockTime = OffsetDateTime.now();
            for (Job j : nextJobs) {
                j.setProcessingStartedAt(lockTime);
                j.setFinishedAt(null);
                j.setFailedAt(null);
                j.setLockedAt(lockTime);
                j.setLockedBy(nodeId);
                j.setUpdatedAt(lockTime);
            }
            jobRepository.saveAll(nextJobs);
            return nextJobs;
        });

        if (jobs == null || jobs.isEmpty()) {
            return;
        }

        // Process each job in parallel
        for (Job job : jobs) {
            processingExecutor.execute(() -> processJob(job, worker));
        }
    }

    private int availableProcessingSlots() {
        int inFlight = processingExecutor.getActiveCount() + processingExecutor.getQueue().size();
        return workerCount - inFlight;
    }

    private void processJob(Job job, JobWorker<?> worker) {
        log.debug("Locked job {} of type {} for processing", job.getId(), job.getType());

        try {
            Object payload = deserializePayload(job, worker);
            invokeWorker(worker, job.getId(), payload);
            markCompleted(job);
            log.debug("Successfully completed job {} of type {}", job.getId(), job.getType());
        } catch (Exception e) {
            if (isExpectedException(worker, e)) {
                log.debug("Job {} of type {} threw expected exception {}. Marking as COMPLETED.",
                        job.getId(), job.getType(), e.getClass().getName());
                markCompleted(job);
                return;
            }

            log.error("Failed to process job {} of type {}", job.getId(), job.getType(), e);
            handleFailure(job, e, worker);
        }
    }

    private Object deserializePayload(Job job, JobWorker<?> worker) throws Exception {
        com.fasterxml.jackson.databind.JsonNode rawPayload = job.getPayload();
        if (rawPayload == null) {
            return null;
        }
        return objectMapper.treeToValue(rawPayload, worker.getPayloadClass());
    }

    private void invokeWorker(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.process(jobId, payload);
    }

    private void markCompleted(Job jobSnapshot) {
        OffsetDateTime now = OffsetDateTime.now();
        Integer updated = transactionTemplate
                .execute(status -> jobRepository.markCompleted(jobSnapshot.getId(), now, nodeId));
        if (toAffectedRows(updated) > 0) {
            scheduleNextRecurringExecutionIfNeeded(jobSnapshot);
            return;
        }

        log.debug("Falling back to entity completion update for job {}", jobSnapshot.getId());
        transactionTemplate.executeWithoutResult(status -> jobRepository.findById(jobSnapshot.getId()).ifPresent(job -> {
            if (!isMutableProcessingJob(job)) {
                log.debug("Skipping completion fallback for job {} due lifecycle/lock mismatch", jobSnapshot.getId());
                return;
            }
            if (job.getProcessingStartedAt() == null) {
                job.setProcessingStartedAt(now);
            }
            job.setFinishedAt(now);
            job.setFailedAt(null);
            job.setErrorMessage(null);
            job.setLockedAt(null);
            job.setLockedBy(null);
            job.setUpdatedAt(now);
            jobRepository.save(job);
            scheduleNextRecurringExecutionIfNeeded(job);
        }));
    }

    private void scheduleNextRecurringExecutionIfNeeded(Job job) {
        if (job.getCron() == null || job.getCron().isBlank()) {
            return;
        }

        try {
            org.springframework.scheduling.support.CronExpression cron = org.springframework.scheduling.support.CronExpression
                    .parse(job.getCron());
            OffsetDateTime nextRun = cron.next(OffsetDateTime.now());
            if (nextRun == null) {
                return;
            }

            Job nextJob = new Job(UUID.randomUUID(), job.getType(), job.getPayload(),
                    job.getMaxRetries(), job.getPriority(), job.getGroupId(), recurringReplaceKey(job.getCron()));
            nextJob.setCron(job.getCron());
            nextJob.setRunAt(nextRun);
            try {
                jobRepository.save(nextJob);
                log.info("Scheduled next execution of recurring job {} at {}", job.getType(), nextRun);
            } catch (DataIntegrityViolationException duplicateSchedule) {
                log.debug("Skipped duplicate recurring schedule for type {} and cron '{}'", job.getType(), job.getCron());
            }
        } catch (Exception cronEx) {
            log.error("Failed to reschedule recurring job {} with cron '{}'",
                    job.getType(), job.getCron(), cronEx);
        }
    }

    private String recurringReplaceKey(String cron) {
        return "__jobq_recurring__:" + cron;
    }

    private void handleFailure(Job jobSnapshot, Exception exception, JobWorker<?> worker) {
        OffsetDateTime now = OffsetDateTime.now();
        int currentRetryCount = jobSnapshot.getRetryCount();
        int nextRetryCount = currentRetryCount + 1;
        String errorMessage = exception.getMessage();

        if (nextRetryCount > jobSnapshot.getMaxRetries()) {
            Integer updated = transactionTemplate.execute(status -> jobRepository.markFailedTerminal(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                return;
            }
        } else {
            RetryDecision retryDecision = computeRetryDecision(jobSnapshot, worker, nextRetryCount, now);
            Integer updated = transactionTemplate.execute(status -> jobRepository.markForRetry(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    retryDecision.nextRunAt(),
                    retryDecision.nextPriority(),
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                return;
            }
        }

        log.debug("Falling back to entity failure update for job {}", jobSnapshot.getId());
        fallbackFailureUpdate(jobSnapshot.getId(), exception, worker);
    }

    private void fallbackFailureUpdate(UUID jobId, Exception exception, JobWorker<?> worker) {
        transactionTemplate.executeWithoutResult(status -> jobRepository.findById(jobId).ifPresent(job -> {
            if (!isMutableProcessingJob(job)) {
                log.debug("Skipping failure fallback for job {} due lifecycle/lock mismatch", jobId);
                return;
            }
            OffsetDateTime now = OffsetDateTime.now();
            job.setErrorMessage(exception.getMessage());
            job.incrementRetryCount();
            job.setUpdatedAt(now);

            if (job.getRetryCount() > job.getMaxRetries()) {
                if (job.getProcessingStartedAt() == null) {
                    job.setProcessingStartedAt(now);
                }
                job.setFailedAt(now);
                job.setFinishedAt(null);
                job.setLockedAt(null);
                job.setLockedBy(null);
            } else {
                RetryDecision retryDecision = computeRetryDecision(job, worker, job.getRetryCount(), now);
                job.setProcessingStartedAt(null);
                job.setFinishedAt(null);
                job.setFailedAt(null);
                job.setLockedAt(null);
                job.setLockedBy(null);
                job.setRunAt(retryDecision.nextRunAt());
                job.setPriority(retryDecision.nextPriority());
            }
            jobRepository.save(job);
        }));
    }

    private RetryDecision computeRetryDecision(Job job, JobWorker<?> worker, int retryCount, OffsetDateTime now) {
        // Parse @Job annotation to compute backoff and priority shifts,
        // resolving user class first so proxy wrappers do not hide annotations.
        com.jobq.annotation.Job jobAnnotation = findJobAnnotation(worker);
        int nextPriority = job.getPriority();
        OffsetDateTime nextRunAt;

        if (jobAnnotation != null) {
            long delayMs = (long) (jobAnnotation.initialBackoffMs()
                    * Math.pow(jobAnnotation.backoffMultiplier(), retryCount - 1));
            nextRunAt = now.plusNanos(delayMs * 1_000_000);

            if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY) {
                nextPriority = job.getPriority() - 1;
            } else if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY) {
                nextPriority = job.getPriority() + 1;
            }
            return new RetryDecision(nextRunAt, nextPriority);
        }

        long delaySeconds = (long) Math.pow(properties.getJobs().getRetryBackOffTimeSeed(),
                Math.max(1, retryCount));
        nextRunAt = now.plusSeconds(delaySeconds);
        return new RetryDecision(nextRunAt, nextPriority);
    }

    private boolean isExpectedException(JobWorker<?> worker, Exception exception) {
        com.jobq.annotation.Job jobAnnotation = findJobAnnotation(worker);
        if (jobAnnotation == null || jobAnnotation.expectedExceptions().length == 0) {
            return false;
        }

        Throwable current = exception;
        while (current != null) {
            for (Class<? extends Throwable> expected : jobAnnotation.expectedExceptions()) {
                if (expected.isAssignableFrom(current.getClass())) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private com.jobq.annotation.Job findJobAnnotation(JobWorker<?> worker) {
        if (workerAnnotationCache.containsKey(worker)) {
            return workerAnnotationCache.get(worker);
        }
        return findJobAnnotationOnClass(worker);
    }

    private com.jobq.annotation.Job findJobAnnotationOnClass(JobWorker<?> worker) {
        Class<?> targetClass = ClassUtils.getUserClass(worker);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private int toAffectedRows(Integer updatedRows) {
        return updatedRows == null ? 0 : updatedRows;
    }

    private boolean isMutableProcessingJob(Job job) {
        return job.getProcessingStartedAt() != null
                && job.getFinishedAt() == null
                && job.getFailedAt() == null
                && job.getLockedAt() != null
                && nodeId.equals(job.getLockedBy());
    }

    private record RetryDecision(OffsetDateTime nextRunAt, int nextPriority) {
    }

    @PreDestroy
    void shutdownExecutor() {
        pollingExecutor.shutdown();
        processingExecutor.shutdown();
    }
}
