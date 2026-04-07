package com.jobq.annotation;

import java.lang.annotation.*;

/**
 * Indicates that the annotated component is a JobQ job and configures its
 * execution behavior (retries, backoff, queueing priority).
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Job {

    /**
     * Optional explicit type of job this processor handles.
     * If blank, JobQ uses the fully-qualified class name as the job type.
     */
    String value() default "";

    /**
     * Optional payload class for annotation-driven jobs. When left as
     * {@code Void.class}, JobQ will infer the payload parameter from the
     * {@code process(...)} method signature when possible.
     */
    Class<?> payload() default Void.class;

    /**
     * A cron expression for recurring jobs. If provided, the job will be
     * automatically
     * rescheduled upon successful completion.
     */
    String cron() default "";

    /**
     * Defines how recurring jobs behave when executions were missed while the
     * application was down or the job cadence drifted behind wall clock time.
     */
    CronMisfirePolicy cronMisfirePolicy() default CronMisfirePolicy.SKIP;

    /**
     * Maximum number of overdue recurring executions JobQ should backfill on
     * startup for {@link #cronMisfirePolicy()} values that support catch-up.
     */
    int maxCatchUpExecutions() default 24;

    /**
     * Initial delay in milliseconds applied when a job is enqueued without an
     * explicit run instant.
     */
    long initialDelayMs() default 0;

    /**
     * Defines what happens to {@code runAt} when a pending deduplicated job is
     * replaced via {@code replaceKey}.
     */
    DeduplicationRunAtPolicy deduplicationRunAtPolicy() default DeduplicationRunAtPolicy.UPDATE_ON_REPLACE;

    /**
     * Defines how grouped pending jobs align their schedule.
     */
    GroupDelayPolicy groupDelayPolicy() default GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE;

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
     * Maximum allowed execution time in milliseconds for a claimed job attempt.
     * When exceeded, the attempt is fenced off and retried or failed terminally.
     * A value of {@code 0} disables execution timeout handling.
     */
    long maxExecutionMs() default 0;

    /**
     * Strategies for how retries affect queue priority.
     */
    RetryPriority retryPriority() default RetryPriority.NORMAL;

    /**
     * Exceptions that should be treated as an expected business outcome.
     * If one of these is thrown during processing, the job is marked COMPLETED
     * and no retry is scheduled.
     */
    Class<? extends Throwable>[] expectedExceptions() default {};

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

    enum DeduplicationRunAtPolicy {
        /**
         * Replacement updates {@code runAt} to the newly computed schedule.
         */
        UPDATE_ON_REPLACE,

        /**
         * Replacement keeps the existing {@code runAt}.
         */
        KEEP_EXISTING
    }

    enum GroupDelayPolicy {
        /**
         * Whenever a grouped job is enqueued, all active pending jobs of the same
         * type/group are synchronized to that enqueue's {@code runAt}.
         */
        SYNC_WITH_NEW_DELAY,

        /**
         * Existing grouped jobs keep their own delay. Once the first grouped job
         * becomes due, all other pending jobs in that group are released to run
         * immediately.
         */
        KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE
    }

    enum CronMisfirePolicy {
        /**
         * Schedule the next future occurrence and skip missed executions.
         */
        SKIP,

        /**
         * If one or more executions were missed, enqueue one immediate recovery run
         * and continue from the current wall clock time afterward.
         */
        FIRE_ONCE,

        /**
         * Continue scheduling from the last expected run time so overdue executions
         * are caught up sequentially.
         */
        CATCH_UP
    }
}
