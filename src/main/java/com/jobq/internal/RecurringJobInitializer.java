package com.jobq.internal;

import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

/**
 * Bootstraps recurring jobs defined via @Job(cron = "...") on startup.
 * Ensures at least one execution is scheduled if none are currently active.
 */
@Component
public class RecurringJobInitializer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RecurringJobInitializer.class);

    private final JobRepository jobRepository;
    private final List<JobWorker<?>> workers;
    private final ListableBeanFactory beanFactory;
    private boolean running = false;

    public RecurringJobInitializer(
            JobRepository jobRepository, List<JobWorker<?>> workers, ListableBeanFactory beanFactory) {
        this.jobRepository = jobRepository;
        this.workers = workers;
        this.beanFactory = beanFactory;
    }

    @Override
    public void start() {
        log.info("Checking for recurring jobs to bootstrap...");
        Map<String, RecurringDefinition> recurringJobs = new LinkedHashMap<>();

        for (JobWorker<?> worker : workers) {
            com.jobq.annotation.Job jobAnnotation = findJobAnnotation(worker);
            if (jobAnnotation != null && !jobAnnotation.cron().isBlank()) {
                registerRecurringDefinition(recurringJobs, worker.getJobType(), jobAnnotation);
            }
        }

        Map<String, Object> annotatedBeans = beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class);
        for (Object bean : annotatedBeans.values()) {
            if (bean instanceof JobWorker<?>) {
                continue;
            }
            com.jobq.annotation.Job jobAnnotation = findJobAnnotation(bean);
            if (jobAnnotation == null || jobAnnotation.cron().isBlank()) {
                continue;
            }
            String type = resolveConfiguredTypeOrClassName(jobAnnotation.value(), ClassUtils.getUserClass(bean));
            registerRecurringDefinition(recurringJobs, type, jobAnnotation);
        }

        for (RecurringDefinition recurringDefinition : recurringJobs.values()) {
            bootstrapRecurringJob(recurringDefinition);
        }

        this.running = true;
    }

    private void registerRecurringDefinition(
            Map<String, RecurringDefinition> recurringJobs, String type, com.jobq.annotation.Job jobAnnotation) {
        RecurringDefinition definition = new RecurringDefinition(
                type,
                jobAnnotation.cron(),
                jobAnnotation.maxRetries(),
                jobAnnotation.cronMisfirePolicy(),
                jobAnnotation.maxCatchUpExecutions());
        RecurringDefinition existing = recurringJobs.putIfAbsent(type, definition);
        if (existing != null
                && (!existing.cron().equals(definition.cron())
                        || existing.maxRetries() != definition.maxRetries()
                        || existing.cronMisfirePolicy() != definition.cronMisfirePolicy()
                        || existing.maxCatchUpExecutions() != definition.maxCatchUpExecutions())) {
            throw new IllegalStateException("Recurring job type '" + type
                    + "' is configured multiple times with different cron/retry settings.");
        }
    }

    private com.jobq.annotation.Job findJobAnnotation(Object bean) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private void bootstrapRecurringJob(RecurringDefinition definition) {
        String type = definition.type();
        String cronExpression = definition.cron();
        try {
            boolean activeExists =
                    jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                            type, cronExpression);
            if (!activeExists) {
                CronExpression cron = CronExpression.parse(cronExpression);
                OffsetDateTime nextRun = resolveBootstrapRunAt(definition, cron, OffsetDateTime.now());

                if (nextRun != null) {
                    Job job = new Job(
                            UUID.randomUUID(),
                            type,
                            null,
                            definition.maxRetries(),
                            0,
                            null,
                            recurringReplaceKey(cronExpression));
                    job.setCron(cronExpression);
                    job.setRunAt(nextRun);
                    try {
                        jobRepository.save(job);
                        log.info(
                                "Bootstrapped recurring job {} with cron '{}'. First execution scheduled at {}",
                                type,
                                cronExpression,
                                nextRun);
                    } catch (DataIntegrityViolationException duplicateSchedule) {
                        log.debug(
                                "Skipped duplicate recurring bootstrap for type {} and cron '{}'",
                                type,
                                cronExpression);
                    }
                }
            } else {
                log.debug("Recurring job {} already has an active execution scheduled.", type);
            }
        } catch (Exception e) {
            log.error("Failed to bootstrap recurring job {} with cron '{}'", type, cronExpression, e);
        }
    }

    private OffsetDateTime resolveBootstrapRunAt(
            RecurringDefinition definition, CronExpression cron, OffsetDateTime now) {
        Job latest = jobRepository.findTopByTypeAndCronOrderByRunAtDesc(definition.type(), definition.cron());
        if (latest == null || latest.getRunAt() == null) {
            return cron.next(now);
        }

        OffsetDateTime expectedNext = cron.next(latest.getRunAt());
        if (expectedNext == null) {
            return null;
        }

        return switch (definition.cronMisfirePolicy()) {
            case SKIP -> {
                OffsetDateTime nextFuture = cron.next(now);
                yield nextFuture == null ? expectedNext : nextFuture;
            }
            case FIRE_ONCE -> expectedNext.isBefore(now) ? now : expectedNext;
            case CATCH_UP -> capCatchUpAnchor(expectedNext, now, definition.maxCatchUpExecutions(), cron);
        };
    }

    private OffsetDateTime capCatchUpAnchor(
            OffsetDateTime candidate, OffsetDateTime now, int maxCatchUpExecutions, CronExpression cron) {
        OffsetDateTime current = candidate;
        OffsetDateTime newestWithinLimit = candidate;
        int observed = 0;
        while (current != null && !current.isAfter(now) && observed < maxCatchUpExecutions) {
            newestWithinLimit = current;
            current = cron.next(current);
            observed++;
        }
        return newestWithinLimit;
    }

    @Override
    public void stop() {
        this.running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE; // Start last
    }

    private String recurringReplaceKey(String cronExpression) {
        return "__jobq_recurring__:" + cronExpression;
    }

    private String resolveConfiguredTypeOrClassName(String configuredType, Class<?> ownerClass) {
        String normalized = configuredType == null ? "" : configuredType.trim();
        if (!normalized.isEmpty()) {
            return normalized;
        }
        return ClassUtils.getUserClass(ownerClass).getName();
    }

    private record RecurringDefinition(
            String type,
            String cron,
            int maxRetries,
            com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicy,
            int maxCatchUpExecutions) {}
}
