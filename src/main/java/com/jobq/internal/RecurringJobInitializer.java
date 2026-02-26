package com.jobq.internal;

import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
            JobRepository jobRepository,
            List<JobWorker<?>> workers,
            ListableBeanFactory beanFactory) {
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
            String type = jobAnnotation.value() == null ? "" : jobAnnotation.value().trim();
            if (type.isBlank()) {
                throw new IllegalStateException("@Job value must not be blank on " + ClassUtils.getUserClass(bean).getName());
            }
            registerRecurringDefinition(recurringJobs, type, jobAnnotation);
        }

        for (RecurringDefinition recurringDefinition : recurringJobs.values()) {
            bootstrapRecurringJob(recurringDefinition.type(), recurringDefinition.cron(), recurringDefinition.maxRetries());
        }

        this.running = true;
    }

    private void registerRecurringDefinition(
            Map<String, RecurringDefinition> recurringJobs,
            String type,
            com.jobq.annotation.Job jobAnnotation) {
        RecurringDefinition definition = new RecurringDefinition(type, jobAnnotation.cron(), jobAnnotation.maxRetries());
        RecurringDefinition existing = recurringJobs.putIfAbsent(type, definition);
        if (existing != null && (!existing.cron().equals(definition.cron()) || existing.maxRetries() != definition.maxRetries())) {
            throw new IllegalStateException(
                    "Recurring job type '" + type + "' is configured multiple times with different cron/retry settings.");
        }
    }

    private com.jobq.annotation.Job findJobAnnotation(Object bean) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private void bootstrapRecurringJob(String type, String cronExpression, int maxRetries) {
        try {
            boolean activeExists = jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(type,
                    cronExpression);
            if (!activeExists) {
                CronExpression cron = CronExpression.parse(cronExpression);
                OffsetDateTime nextRun = cron.next(OffsetDateTime.now());

                if (nextRun != null) {
                    Job job = new Job(UUID.randomUUID(), type, null, maxRetries, 0, null,
                            recurringReplaceKey(cronExpression));
                    job.setCron(cronExpression);
                    job.setRunAt(nextRun);
                    try {
                        jobRepository.save(job);
                        log.info("Bootstrapped recurring job {} with cron '{}'. First execution scheduled at {}", type,
                                cronExpression, nextRun);
                    } catch (DataIntegrityViolationException duplicateSchedule) {
                        log.debug("Skipped duplicate recurring bootstrap for type {} and cron '{}'",
                                type, cronExpression);
                    }
                }
            } else {
                log.debug("Recurring job {} already has an active execution scheduled.", type);
            }
        } catch (Exception e) {
            log.error("Failed to bootstrap recurring job {} with cron '{}'", type, cronExpression, e);
        }
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

    private record RecurringDefinition(String type, String cron, int maxRetries) {
    }
}
