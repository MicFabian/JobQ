package com.jobq.annotation;

import java.lang.annotation.*;

/**
 * Indicates that the annotated component is a JobWorker and configures
 * its execution behavior (retries, backoff, queueing priority).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Job {

    /**
     * The type of job this worker handles.
     */
    String value();

    /**
     * The maximum number of retries before a job is marked as FAILED permanently.
     */
    int maxRetries() default 3;

    /**
     * The multiplier used for exponential backoff between retries.
     */
    double backoffMultiplier() default 2.0;

    /**
     * The initial delay in milliseconds before the first retry.
     */
    long initialBackoffMs() default 1000;

    /**
     * Strategies for how retries affect queue priority.
     */
    RetryPriority retryPriority() default RetryPriority.NORMAL;

    enum RetryPriority {
        /**
         * Retries are pushed to the back of the queue (priority decreases as retries
         * increase).
         */
        LOWER_ON_RETRY,

        /**
         * Retries maintain their original priority.
         */
        NORMAL,

        /**
         * Retries are prioritized to clear them out faster (priority increases as
         * retries increase).
         */
        HIGHER_ON_RETRY
    }
}
