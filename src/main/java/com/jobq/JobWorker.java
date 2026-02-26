package com.jobq;

/**
 * Interface representing a worker that can process jobs of a specific type.
 * Note: The class must be registered as a Spring Bean to be detected by the
 * JobPoller.
 *
 * @param <T> the type of the payload expected by this worker
 */
public interface JobWorker<T> {

    /**
     * Returns the globally unique type of job this worker is capable of processing.
     * By default, this is extracted from the {@link com.jobq.annotation.Job}
     * annotation.
     */
    default String getJobType() {
        Class<?> targetClass = org.springframework.util.ClassUtils.getUserClass(this);
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
     * Optionally returns the payload class so the generic payload can be explicitly
     * converted dynamically.
     */
    Class<T> getPayloadClass();
}
