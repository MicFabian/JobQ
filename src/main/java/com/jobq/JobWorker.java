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
     * Ensure this matches the type provided when enqueueing a job.
     */
    String getJobType();

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
