package com.jobq;

import java.util.UUID;

/**
 * Optional structured lifecycle contract for annotation-driven {@code @Job}
 * classes.
 *
 * <p>Implement this interface when you want compile-time structure for job
 * lifecycle callbacks while still using annotation-driven registration.
 *
 * @param <T> payload type
 */
public interface JobLifecycle<T> {

    /**
     * Main job execution callback.
     */
    void process(UUID jobId, T payload) throws Exception;

    /**
     * Optional callback invoked when {@code process(...)} throws.
     */
    default void onError(UUID jobId, T payload, Exception exception) {
        // no-op
    }

    /**
     * Optional callback invoked after successful execution.
     */
    default void onSuccess(UUID jobId, T payload) {
        // no-op
    }

    /**
     * Optional callback invoked after completion, regardless of outcome.
     */
    default void after(UUID jobId, T payload) {
        // no-op
    }
}
