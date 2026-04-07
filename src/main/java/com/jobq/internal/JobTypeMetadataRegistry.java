package com.jobq.internal;

import com.jobq.JobWorker;
import jakarta.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

@Component
public class JobTypeMetadataRegistry {

    private final List<JobWorker<?>> workers;
    private final ListableBeanFactory beanFactory;
    private volatile Map<String, JobTypeMetadata> metadataByType = Map.of();
    private volatile Map<Class<?>, String> jobTypeByClass = Map.of();

    public JobTypeMetadataRegistry(List<JobWorker<?>> workers, ListableBeanFactory beanFactory) {
        this.workers = workers;
        this.beanFactory = beanFactory;
    }

    @PostConstruct
    void init() {
        Map<String, JobTypeMetadata> metadata = new LinkedHashMap<>();
        Map<Class<?>, String> classMappings = new LinkedHashMap<>();

        for (JobWorker<?> worker : workers) {
            Class<?> workerClass = ClassUtils.getUserClass(worker);
            String jobType = normalizeRequiredType(worker.getJobType(), "JobWorker " + workerClass.getName());
            com.jobq.annotation.Job jobAnnotation = findJobAnnotation(worker);
            long initialDelayMs = jobAnnotation != null ? sanitizeInitialDelayMs(jobAnnotation.initialDelayMs()) : 0L;
            long maxExecutionMs = jobAnnotation != null ? sanitizeMaxExecutionMs(jobAnnotation.maxExecutionMs()) : 0L;
            Integer annotationMaxRetries =
                    jobAnnotation != null ? sanitizeMaxRetries(jobAnnotation.maxRetries()) : null;
            String recurringCron = sanitizeRecurringCron(jobAnnotation);
            com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicy = jobAnnotation != null
                    ? jobAnnotation.cronMisfirePolicy()
                    : com.jobq.annotation.Job.CronMisfirePolicy.SKIP;
            int maxCatchUpExecutions =
                    jobAnnotation != null ? sanitizeMaxCatchUpExecutions(jobAnnotation.maxCatchUpExecutions()) : 24;
            com.jobq.annotation.Job.DeduplicationRunAtPolicy deduplicationRunAtPolicy = jobAnnotation != null
                    ? jobAnnotation.deduplicationRunAtPolicy()
                    : com.jobq.annotation.Job.DeduplicationRunAtPolicy.UPDATE_ON_REPLACE;
            com.jobq.annotation.Job.GroupDelayPolicy groupDelayPolicy = jobAnnotation != null
                    ? jobAnnotation.groupDelayPolicy()
                    : com.jobq.annotation.Job.GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE;
            register(
                    metadata,
                    jobType,
                    initialDelayMs,
                    maxExecutionMs,
                    recurringCron,
                    cronMisfirePolicy,
                    maxCatchUpExecutions,
                    deduplicationRunAtPolicy,
                    groupDelayPolicy,
                    annotationMaxRetries,
                    "JobWorker bean " + ClassUtils.getUserClass(worker).getName());
            registerClassMapping(classMappings, workerClass, jobType, "JobWorker bean " + workerClass.getName());
        }

        Map<String, Object> annotationBeans = beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class);
        for (Object bean : annotationBeans.values()) {
            if (bean instanceof JobWorker<?>) {
                continue;
            }

            com.jobq.annotation.Job jobAnnotation = findJobAnnotation(bean);
            if (jobAnnotation == null) {
                continue;
            }

            Class<?> beanClass = ClassUtils.getUserClass(bean);
            String jobType = resolveConfiguredTypeOrClassName(
                    jobAnnotation.value(), beanClass, "@Job bean " + beanClass.getName());

            long initialDelayMs = sanitizeInitialDelayMs(jobAnnotation.initialDelayMs());
            long maxExecutionMs = sanitizeMaxExecutionMs(jobAnnotation.maxExecutionMs());
            register(
                    metadata,
                    jobType,
                    initialDelayMs,
                    maxExecutionMs,
                    sanitizeRecurringCron(jobAnnotation),
                    jobAnnotation.cronMisfirePolicy(),
                    sanitizeMaxCatchUpExecutions(jobAnnotation.maxCatchUpExecutions()),
                    jobAnnotation.deduplicationRunAtPolicy(),
                    jobAnnotation.groupDelayPolicy(),
                    sanitizeMaxRetries(jobAnnotation.maxRetries()),
                    "@Job bean " + ClassUtils.getUserClass(bean).getName());
            registerClassMapping(classMappings, beanClass, jobType, "@Job bean " + beanClass.getName());
        }

        metadataByType = Map.copyOf(metadata);
        jobTypeByClass = Map.copyOf(classMappings);
    }

    public long initialDelayMsFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? 0L : metadata.initialDelayMs();
    }

    public Set<String> registeredJobTypes() {
        return metadataByType.keySet();
    }

    public com.jobq.annotation.Job.DeduplicationRunAtPolicy deduplicationRunAtPolicyFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null
                ? com.jobq.annotation.Job.DeduplicationRunAtPolicy.UPDATE_ON_REPLACE
                : metadata.deduplicationRunAtPolicy();
    }

    public com.jobq.annotation.Job.GroupDelayPolicy groupDelayPolicyFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null
                ? com.jobq.annotation.Job.GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE
                : metadata.groupDelayPolicy();
    }

    public long maxExecutionMsFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? 0L : metadata.maxExecutionMs();
    }

    public String recurringCronFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? null : metadata.recurringCron();
    }

    public com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicyFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? com.jobq.annotation.Job.CronMisfirePolicy.SKIP : metadata.cronMisfirePolicy();
    }

    public int maxCatchUpExecutionsFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? 24 : metadata.maxCatchUpExecutions();
    }

    public Map<String, RecurringJobMetadata> recurringMetadata() {
        Map<String, RecurringJobMetadata> recurring = new LinkedHashMap<>();
        for (Map.Entry<String, JobTypeMetadata> entry : metadataByType.entrySet()) {
            JobTypeMetadata metadata = entry.getValue();
            if (metadata.recurringCron() == null || metadata.recurringCron().isBlank()) {
                continue;
            }
            recurring.put(
                    entry.getKey(),
                    new RecurringJobMetadata(
                            entry.getKey(),
                            metadata.recurringCron(),
                            metadata.cronMisfirePolicy(),
                            metadata.maxCatchUpExecutions(),
                            metadata.annotationMaxRetries()));
        }
        return Map.copyOf(recurring);
    }

    public int defaultMaxRetriesFor(String jobType, int fallbackMaxRetries) {
        if (fallbackMaxRetries < 0) {
            throw new IllegalArgumentException("fallbackMaxRetries must be >= 0");
        }
        JobTypeMetadata metadata = metadataByType.get(jobType);
        if (metadata == null || metadata.annotationMaxRetries() == null) {
            return fallbackMaxRetries;
        }
        return metadata.annotationMaxRetries();
    }

    public String jobTypeFor(Class<?> jobClass) {
        if (jobClass == null) {
            throw new IllegalArgumentException("Job class must not be null");
        }
        Class<?> targetClass = ClassUtils.getUserClass(jobClass);
        String mappedType = jobTypeByClass.get(targetClass);
        if (mappedType != null) {
            return mappedType;
        }

        com.jobq.annotation.Job annotation = AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Job class " + targetClass.getName()
                    + " is not a registered JobQ bean and has no @Job annotation. "
                    + "Use enqueue(String, payload) or annotate/register the class.");
        }
        return resolveConfiguredTypeOrClassName(annotation.value(), targetClass, "Job class " + targetClass.getName());
    }

    private void register(
            Map<String, JobTypeMetadata> metadata,
            String jobType,
            long initialDelayMs,
            long maxExecutionMs,
            String recurringCron,
            com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicy,
            int maxCatchUpExecutions,
            com.jobq.annotation.Job.DeduplicationRunAtPolicy deduplicationRunAtPolicy,
            com.jobq.annotation.Job.GroupDelayPolicy groupDelayPolicy,
            Integer annotationMaxRetries,
            String source) {
        JobTypeMetadata existing = metadata.putIfAbsent(
                jobType,
                new JobTypeMetadata(
                        initialDelayMs,
                        maxExecutionMs,
                        recurringCron,
                        cronMisfirePolicy,
                        maxCatchUpExecutions,
                        deduplicationRunAtPolicy,
                        groupDelayPolicy,
                        annotationMaxRetries));
        if (existing != null
                && (existing.initialDelayMs() != initialDelayMs
                        || existing.maxExecutionMs() != maxExecutionMs
                        || !Objects.equals(existing.recurringCron(), recurringCron)
                        || existing.cronMisfirePolicy() != cronMisfirePolicy
                        || existing.maxCatchUpExecutions() != maxCatchUpExecutions
                        || existing.deduplicationRunAtPolicy() != deduplicationRunAtPolicy
                        || existing.groupDelayPolicy() != groupDelayPolicy
                        || !Objects.equals(existing.annotationMaxRetries(), annotationMaxRetries))) {
            throw new IllegalStateException("Job type '" + jobType
                    + "' has conflicting metadata between definitions while scanning "
                    + source + ".");
        }
    }

    private com.jobq.annotation.Job findJobAnnotation(Object bean) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private String normalizeRequiredType(String type, String source) {
        if (type == null) {
            throw new IllegalStateException("Job type must not be null for " + source);
        }
        String trimmed = type.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Job type must not be blank for " + source);
        }
        return trimmed;
    }

    private String resolveConfiguredTypeOrClassName(String configuredType, Class<?> ownerClass, String source) {
        String normalized = configuredType == null ? "" : configuredType.trim();
        if (!normalized.isEmpty()) {
            return normalizeRequiredType(normalized, source);
        }
        Class<?> targetClass = ClassUtils.getUserClass(ownerClass);
        return normalizeRequiredType(targetClass.getName(), source);
    }

    private long sanitizeInitialDelayMs(long initialDelayMs) {
        if (initialDelayMs < 0) {
            throw new IllegalStateException("@Job initialDelayMs must be >= 0");
        }
        return initialDelayMs;
    }

    private int sanitizeMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalStateException("@Job maxRetries must be >= 0");
        }
        return maxRetries;
    }

    private long sanitizeMaxExecutionMs(long maxExecutionMs) {
        if (maxExecutionMs < 0) {
            throw new IllegalStateException("@Job maxExecutionMs must be >= 0");
        }
        return maxExecutionMs;
    }

    private int sanitizeMaxCatchUpExecutions(int maxCatchUpExecutions) {
        if (maxCatchUpExecutions <= 0) {
            throw new IllegalStateException("@Job maxCatchUpExecutions must be > 0");
        }
        return maxCatchUpExecutions;
    }

    private String sanitizeRecurringCron(com.jobq.annotation.Job jobAnnotation) {
        if (jobAnnotation == null
                || jobAnnotation.cron() == null
                || jobAnnotation.cron().isBlank()) {
            return null;
        }
        return jobAnnotation.cron().trim();
    }

    private void registerClassMapping(
            Map<Class<?>, String> classMappings, Class<?> sourceClass, String jobType, String source) {
        String existingType = classMappings.putIfAbsent(sourceClass, jobType);
        if (existingType != null && !existingType.equals(jobType)) {
            throw new IllegalStateException("Class " + sourceClass.getName() + " maps to multiple job types ("
                    + existingType + ", " + jobType + ") while scanning " + source + ".");
        }
    }

    private record JobTypeMetadata(
            long initialDelayMs,
            long maxExecutionMs,
            String recurringCron,
            com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicy,
            int maxCatchUpExecutions,
            com.jobq.annotation.Job.DeduplicationRunAtPolicy deduplicationRunAtPolicy,
            com.jobq.annotation.Job.GroupDelayPolicy groupDelayPolicy,
            Integer annotationMaxRetries) {}

    public record RecurringJobMetadata(
            String type,
            String cron,
            com.jobq.annotation.Job.CronMisfirePolicy cronMisfirePolicy,
            int maxCatchUpExecutions,
            Integer maxRetries) {}
}
