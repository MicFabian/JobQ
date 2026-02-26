package com.jobq.internal;

import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.time.OffsetDateTime;
import java.util.List;
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
    private boolean running = false;

    public RecurringJobInitializer(JobRepository jobRepository, List<JobWorker<?>> workers) {
        this.jobRepository = jobRepository;
        this.workers = workers;
    }

    @Override
    public void start() {
        log.info("Checking for recurring jobs to bootstrap...");
        for (JobWorker<?> worker : workers) {
            Class<?> targetClass = ClassUtils.getUserClass(worker);
            com.jobq.annotation.Job jobAnnotation = AnnotationUtils.findAnnotation(targetClass,
                    com.jobq.annotation.Job.class);
            if (jobAnnotation != null && !jobAnnotation.cron().isBlank()) {
                bootstrapRecurringJob(worker.getJobType(), jobAnnotation.cron(), jobAnnotation.maxRetries());
            }
        }
        this.running = true;
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
}
