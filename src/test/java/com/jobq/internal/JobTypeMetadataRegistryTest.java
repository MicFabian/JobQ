package com.jobq.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.JobLifecycle;
import com.jobq.JobWorker;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

class JobTypeMetadataRegistryTest {

    @com.jobq.annotation.Job(
            value = "METADATA_WORKER_JOB",
            cron = "*/5 * * * * *",
            cronMisfirePolicy = com.jobq.annotation.Job.CronMisfirePolicy.CATCH_UP,
            maxCatchUpExecutions = 5,
            initialDelayMs = 250,
            maxExecutionMs = 900,
            maxRetries = 7,
            deduplicationRunAtPolicy = com.jobq.annotation.Job.DeduplicationRunAtPolicy.KEEP_EXISTING,
            groupDelayPolicy = com.jobq.annotation.Job.GroupDelayPolicy.SYNC_WITH_NEW_DELAY)
    static class MetadataWorker implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(payload = String.class, maxRetries = 4)
    static class AnnotationLifecycleJob implements JobLifecycle<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(value = "CONFLICTING_JOB", initialDelayMs = 1)
    static class ConflictingWorkerOne implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(value = "CONFLICTING_JOB", initialDelayMs = 2)
    static class ConflictingWorkerTwo implements JobWorker<String> {
        @Override
        public void process(UUID jobId, String payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job
    static class UnregisteredAnnotatedJob {}

    @Test
    void shouldExposeMetadataForWorkersAndAnnotationOnlyBeans() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("annotationLifecycleJob", AnnotationLifecycleJob.class, AnnotationLifecycleJob::new);
        context.refresh();

        try {
            JobTypeMetadataRegistry registry = new JobTypeMetadataRegistry(List.of(new MetadataWorker()), context);
            registry.init();

            assertEquals(
                    Set.of("METADATA_WORKER_JOB", AnnotationLifecycleJob.class.getSimpleName()),
                    registry.registeredJobTypes());
            assertEquals(250L, registry.initialDelayMsFor("METADATA_WORKER_JOB"));
            assertEquals(900L, registry.maxExecutionMsFor("METADATA_WORKER_JOB"));
            assertEquals("*/5 * * * * *", registry.recurringCronFor("METADATA_WORKER_JOB"));
            assertEquals(
                    com.jobq.annotation.Job.CronMisfirePolicy.CATCH_UP,
                    registry.cronMisfirePolicyFor("METADATA_WORKER_JOB"));
            assertEquals(5, registry.maxCatchUpExecutionsFor("METADATA_WORKER_JOB"));
            assertEquals(
                    com.jobq.annotation.Job.DeduplicationRunAtPolicy.KEEP_EXISTING,
                    registry.deduplicationRunAtPolicyFor("METADATA_WORKER_JOB"));
            assertEquals(
                    com.jobq.annotation.Job.GroupDelayPolicy.SYNC_WITH_NEW_DELAY,
                    registry.groupDelayPolicyFor("METADATA_WORKER_JOB"));
            assertEquals(7, registry.defaultMaxRetriesFor("METADATA_WORKER_JOB", 3));
            assertEquals(4, registry.defaultMaxRetriesFor(AnnotationLifecycleJob.class.getSimpleName(), 3));
            assertEquals("METADATA_WORKER_JOB", registry.jobTypeFor(MetadataWorker.class));
            assertEquals(
                    AnnotationLifecycleJob.class.getSimpleName(), registry.jobTypeFor(AnnotationLifecycleJob.class));

            JobTypeMetadataRegistry.RecurringJobMetadata recurringJobMetadata =
                    registry.recurringMetadata().get("METADATA_WORKER_JOB");
            assertEquals("METADATA_WORKER_JOB", recurringJobMetadata.type());
            assertEquals(7, recurringJobMetadata.maxRetries());
            assertEquals(5, recurringJobMetadata.maxCatchUpExecutions());

            assertEquals(0L, registry.initialDelayMsFor("UNKNOWN_JOB"));
            assertEquals(0L, registry.maxExecutionMsFor("UNKNOWN_JOB"));
            assertNull(registry.recurringCronFor("UNKNOWN_JOB"));
            assertEquals(12, registry.defaultMaxRetriesFor("UNKNOWN_JOB", 12));
            assertEquals(
                    com.jobq.annotation.Job.DeduplicationRunAtPolicy.UPDATE_ON_REPLACE,
                    registry.deduplicationRunAtPolicyFor("UNKNOWN_JOB"));
            assertEquals(
                    com.jobq.annotation.Job.GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE,
                    registry.groupDelayPolicyFor("UNKNOWN_JOB"));
        } finally {
            context.close();
        }
    }

    @Test
    void shouldRejectConflictingMetadataForSameJobType() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();

        try {
            JobTypeMetadataRegistry registry = new JobTypeMetadataRegistry(
                    List.of(new ConflictingWorkerOne(), new ConflictingWorkerTwo()), context);

            IllegalStateException error = assertThrows(IllegalStateException.class, registry::init);
            assertTrue(error.getMessage().contains("conflicting metadata"));
            assertTrue(error.getMessage().contains("CONFLICTING_JOB"));
        } finally {
            context.close();
        }
    }

    @Test
    void shouldExplainWhyAnnotatedClassCannotBeUsedWhenNotRegistered() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.refresh();

        try {
            JobTypeMetadataRegistry registry = new JobTypeMetadataRegistry(List.of(), context);
            registry.init();

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class, () -> registry.jobTypeFor(UnregisteredAnnotatedJob.class));
            assertTrue(error.getMessage().contains("not registered as a Spring bean"));
            assertTrue(error.getMessage().contains(UnregisteredAnnotatedJob.class.getName()));
        } finally {
            context.close();
        }
    }
}
