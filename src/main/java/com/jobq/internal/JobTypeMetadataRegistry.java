package com.jobq.internal;

import com.jobq.JobWorker;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class JobTypeMetadataRegistry {

    private final List<JobWorker<?>> workers;
    private final ListableBeanFactory beanFactory;
    private volatile Map<String, JobTypeMetadata> metadataByType = Map.of();

    public JobTypeMetadataRegistry(List<JobWorker<?>> workers, ListableBeanFactory beanFactory) {
        this.workers = workers;
        this.beanFactory = beanFactory;
    }

    @PostConstruct
    void init() {
        Map<String, JobTypeMetadata> metadata = new LinkedHashMap<>();

        for (JobWorker<?> worker : workers) {
            String jobType = normalizeRequiredType(worker.getJobType(), "JobWorker " + ClassUtils.getUserClass(worker).getName());
            com.jobq.annotation.Job jobAnnotation = findJobAnnotation(worker);
            long initialDelayMs = jobAnnotation != null ? sanitizeInitialDelayMs(jobAnnotation.initialDelayMs()) : 0L;
            register(metadata, jobType, initialDelayMs, "JobWorker bean " + ClassUtils.getUserClass(worker).getName());
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

            String jobType = normalizeRequiredType(jobAnnotation.value(),
                    "@Job bean " + ClassUtils.getUserClass(bean).getName());

            long initialDelayMs = sanitizeInitialDelayMs(jobAnnotation.initialDelayMs());
            register(metadata, jobType, initialDelayMs, "@Job bean " + ClassUtils.getUserClass(bean).getName());
        }

        metadataByType = Map.copyOf(metadata);
    }

    public long initialDelayMsFor(String jobType) {
        JobTypeMetadata metadata = metadataByType.get(jobType);
        return metadata == null ? 0L : metadata.initialDelayMs();
    }

    private void register(Map<String, JobTypeMetadata> metadata, String jobType, long initialDelayMs, String source) {
        JobTypeMetadata existing = metadata.putIfAbsent(jobType, new JobTypeMetadata(initialDelayMs));
        if (existing != null && existing.initialDelayMs() != initialDelayMs) {
            throw new IllegalStateException(
                    "Job type '" + jobType + "' has conflicting initialDelayMs between definitions while scanning "
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

    private long sanitizeInitialDelayMs(long initialDelayMs) {
        if (initialDelayMs < 0) {
            throw new IllegalStateException("@Job initialDelayMs must be >= 0");
        }
        return initialDelayMs;
    }

    private record JobTypeMetadata(long initialDelayMs) {
    }
}
