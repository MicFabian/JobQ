package com.jobq;

import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interface representing a processor that can execute jobs of a specific type.
 * Note: The class must be registered as a Spring Bean to be detected by the
 * JobPoller.
 *
 * @param <T> the type of the payload expected by this processor
 */
public interface JobWorker<T> {

    Map<Class<?>, Class<?>> PAYLOAD_CLASS_CACHE = new ConcurrentHashMap<>();

    /**
     * Returns the globally unique type of job this worker is capable of processing.
     * By default, this is extracted from the {@link com.jobq.annotation.Job}
     * annotation.
     */
    default String getJobType() {
        Class<?> targetClass = ClassUtils.getUserClass(this);
        com.jobq.annotation.Job annotation = org.springframework.core.annotation.AnnotationUtils
                .findAnnotation(targetClass, com.jobq.annotation.Job.class);
        if (annotation == null || annotation.value().isBlank()) {
            throw new IllegalStateException("JobWorker " + targetClass.getName() +
                    " must either be annotated with @Job or override getJobType()");
        }
        return annotation.value();
    }

    /**
     * Processes a single job.
     * Any exception thrown from this method will mark the job as FAILED (and
     * potentially retried).
     *
     * @param jobId   the unique job ID
     * @param payload the deserialized payload
     * @throws Exception if processing fails
     */
    void process(java.util.UUID jobId, T payload) throws Exception;

    /**
     * Optional callback invoked when {@link #process(UUID, Object)} throws.
     * Throwing from this method does not replace the original processing failure;
     * JobQ logs callback failures and continues normal retry/failure handling.
     */
    default void onError(java.util.UUID jobId, T payload, Exception exception) {
        // no-op
    }

    /**
     * Optional callback invoked after successful execution.
     * Throwing from this method does not change the successful job outcome;
     * JobQ logs callback failures and keeps the job completed.
     */
    default void onSuccess(java.util.UUID jobId, T payload) {
        // no-op
    }

    /**
     * Optional alias for {@link #onSuccess(UUID, Object)}.
     * Override this method if you prefer `after(...)` naming.
     */
    default void after(java.util.UUID jobId, T payload) {
        onSuccess(jobId, payload);
    }

    /**
     * Optionally returns the payload class so the generic payload can be explicitly
     * converted dynamically. By default, this is inferred from {@code JobWorker<T>}.
     */
    @SuppressWarnings("unchecked")
    default Class<T> getPayloadClass() {
        Class<?> targetClass = ClassUtils.getUserClass(this);
        Class<?> payloadClass = PAYLOAD_CLASS_CACHE.computeIfAbsent(targetClass, JobWorker::inferPayloadClass);
        return (Class<T>) payloadClass;
    }

    private static Class<?> inferPayloadClass(Class<?> targetClass) {
        Class<?> resolved = ResolvableType.forClass(targetClass)
                .as(JobWorker.class)
                .getGeneric(0)
                .resolve();
        if (resolved == null) {
            throw new IllegalStateException("JobWorker " + targetClass.getName()
                    + " payload type cannot be inferred. Specify a concrete generic type or override getPayloadClass().");
        }
        return resolved;
    }
}
