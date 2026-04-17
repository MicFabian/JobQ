package com.jobq.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ListableBeanFactory;

class RecurringJobInitializerTest {

    private JobRepository jobRepository;
    private ListableBeanFactory beanFactory;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        beanFactory = mock(ListableBeanFactory.class);
        when(beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class)).thenReturn(Map.of());
    }

    @com.jobq.annotation.Job(value = "RECURRING_BOOTSTRAP_JOB", cron = "*/5 * * * * *", maxRetries = 7)
    static class RecurringWorker implements JobWorker<Void> {
        @Override
        public void process(UUID jobId, Void payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(value = "INVALID_CRON_JOB", cron = "not-a-cron")
    static class InvalidCronWorker implements JobWorker<Void> {
        @Override
        public void process(UUID jobId, Void payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(value = "ANNOTATION_ONLY_RECURRING_JOB", cron = "*/5 * * * * *", maxRetries = 4)
    static class AnnotationOnlyRecurringJob {
        @SuppressWarnings("unused")
        public void process(UUID jobId) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(cron = "*/5 * * * * *", maxRetries = 2)
    static class ClassNameRecurringJob {
        @SuppressWarnings("unused")
        public void process(UUID jobId) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(
            value = "SKIP_MISSED_RECURRING_JOB",
            cron = "*/5 * * * * *",
            cronMisfirePolicy = com.jobq.annotation.Job.CronMisfirePolicy.SKIP)
    static class SkipRecurringWorker implements JobWorker<Void> {
        @Override
        public void process(UUID jobId, Void payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(
            value = "FIRE_ONCE_RECURRING_JOB",
            cron = "*/5 * * * * *",
            cronMisfirePolicy = com.jobq.annotation.Job.CronMisfirePolicy.FIRE_ONCE)
    static class FireOnceRecurringWorker implements JobWorker<Void> {
        @Override
        public void process(UUID jobId, Void payload) {
            // no-op
        }
    }

    @com.jobq.annotation.Job(
            value = "CATCH_UP_RECURRING_JOB",
            cron = "*/5 * * * * *",
            cronMisfirePolicy = com.jobq.annotation.Job.CronMisfirePolicy.CATCH_UP,
            maxCatchUpExecutions = 3)
    static class CatchUpRecurringWorker implements JobWorker<Void> {
        @Override
        public void process(UUID jobId, Void payload) {
            // no-op
        }
    }

    @Test
    void shouldBootstrapRecurringJobWhenNoActiveExecutionExists() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "RECURRING_BOOTSTRAP_JOB", "*/5 * * * * *"))
                .thenReturn(false);

        RecurringJobInitializer initializer =
                new RecurringJobInitializer(jobRepository, List.of(new RecurringWorker()), beanFactory);
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        Job savedJob = captor.getValue();

        assertEquals("RECURRING_BOOTSTRAP_JOB", savedJob.getType());
        assertEquals("*/5 * * * * *", savedJob.getCron());
        assertEquals(7, savedJob.getMaxRetries());
        assertEquals("__jobq_recurring__:*/5 * * * * *", savedJob.getReplaceKey());
        assertNotNull(savedJob.getRunAt());
        assertTrue(savedJob.getRunAt().isAfter(OffsetDateTime.now().minusSeconds(1)));
        assertTrue(initializer.isRunning());
    }

    @Test
    void shouldSkipBootstrappingWhenActiveRecurringExecutionAlreadyExists() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "RECURRING_BOOTSTRAP_JOB", "*/5 * * * * *"))
                .thenReturn(true);

        RecurringJobInitializer initializer =
                new RecurringJobInitializer(jobRepository, List.of(new RecurringWorker()), beanFactory);
        initializer.start();

        verify(jobRepository, never()).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldIgnoreInvalidCronExpressionsWithoutCrashingStartup() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "INVALID_CRON_JOB", "not-a-cron"))
                .thenReturn(false);

        RecurringJobInitializer initializer =
                new RecurringJobInitializer(jobRepository, List.of(new InvalidCronWorker()), beanFactory);

        assertDoesNotThrow(initializer::start);
        verify(jobRepository, never()).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldDetectRecurringAnnotationOnProxiedWorker() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "RECURRING_BOOTSTRAP_JOB", "*/5 * * * * *"))
                .thenReturn(false);

        ProxyFactory proxyFactory = new ProxyFactory(new RecurringWorker());
        proxyFactory.setProxyTargetClass(true);
        @SuppressWarnings("unchecked")
        JobWorker<Void> proxiedWorker = (JobWorker<Void>) proxyFactory.getProxy();

        RecurringJobInitializer initializer =
                new RecurringJobInitializer(jobRepository, List.of(proxiedWorker), beanFactory);
        initializer.start();

        verify(jobRepository).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldBootstrapRecurringJobFromAnnotationOnlyBean() {
        when(beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class))
                .thenReturn(Map.of("annotationOnlyRecurringJob", new AnnotationOnlyRecurringJob()));
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "ANNOTATION_ONLY_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(false);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(), beanFactory);
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals("ANNOTATION_ONLY_RECURRING_JOB", captor.getValue().getType());
        assertEquals(4, captor.getValue().getMaxRetries());
    }

    @Test
    void shouldFallbackRecurringTypeToClassNameWhenAnnotationValueOmitted() {
        when(beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class))
                .thenReturn(Map.of("classNameRecurringJob", new ClassNameRecurringJob()));
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        ClassNameRecurringJob.class.getSimpleName(), "*/5 * * * * *"))
                .thenReturn(false);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(), beanFactory);
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals(
                ClassNameRecurringJob.class.getSimpleName(), captor.getValue().getType());
        assertEquals(2, captor.getValue().getMaxRetries());
    }

    @Test
    void shouldNotCreateDuplicateRecurringJobWhenReconciledAgainWithActiveExecutionPresent() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "RECURRING_BOOTSTRAP_JOB", "*/5 * * * * *"))
                .thenReturn(false, true);

        RecurringJobInitializer initializer =
                new RecurringJobInitializer(jobRepository, List.of(new RecurringWorker()), beanFactory);
        initializer.start();
        initializer.reconcileRecurringJobs();

        verify(jobRepository, times(1)).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldSkipMissedRecurringExecutionsWhenMisfirePolicyIsSkip() {
        OffsetDateTime now = OffsetDateTime.parse("2026-04-16T12:00:20Z");
        OffsetDateTime latestRun = OffsetDateTime.parse("2026-04-16T11:59:50Z");

        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "SKIP_MISSED_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(false);
        when(jobRepository.findTopByTypeAndCronOrderByRunAtDesc("SKIP_MISSED_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(jobWithRunAt("SKIP_MISSED_RECURRING_JOB", "*/5 * * * * *", latestRun));

        RecurringJobInitializer initializer = new RecurringJobInitializer(
                jobRepository, List.of(new SkipRecurringWorker()), beanFactory, fixedClock(now));
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals(
                OffsetDateTime.parse("2026-04-16T12:00:25Z"), captor.getValue().getRunAt());
    }

    @Test
    void shouldFireOnceImmediatelyWhenRecurringExecutionWasMissed() {
        OffsetDateTime now = OffsetDateTime.parse("2026-04-16T12:00:20Z");
        OffsetDateTime latestRun = OffsetDateTime.parse("2026-04-16T11:59:50Z");

        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "FIRE_ONCE_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(false);
        when(jobRepository.findTopByTypeAndCronOrderByRunAtDesc("FIRE_ONCE_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(jobWithRunAt("FIRE_ONCE_RECURRING_JOB", "*/5 * * * * *", latestRun));

        RecurringJobInitializer initializer = new RecurringJobInitializer(
                jobRepository, List.of(new FireOnceRecurringWorker()), beanFactory, fixedClock(now));
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals(now, captor.getValue().getRunAt());
    }

    @Test
    void shouldCatchUpFromOldestRetainedMissedExecution() {
        OffsetDateTime now = OffsetDateTime.parse("2026-04-16T12:00:20Z");
        OffsetDateTime latestRun = OffsetDateTime.parse("2026-04-16T11:59:50Z");

        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNullAndCancelledAtIsNull(
                        "CATCH_UP_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(false);
        when(jobRepository.findTopByTypeAndCronOrderByRunAtDesc("CATCH_UP_RECURRING_JOB", "*/5 * * * * *"))
                .thenReturn(jobWithRunAt("CATCH_UP_RECURRING_JOB", "*/5 * * * * *", latestRun));

        RecurringJobInitializer initializer = new RecurringJobInitializer(
                jobRepository, List.of(new CatchUpRecurringWorker()), beanFactory, fixedClock(now));
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals(
                OffsetDateTime.parse("2026-04-16T12:00:10Z"), captor.getValue().getRunAt());
    }

    private Clock fixedClock(OffsetDateTime now) {
        return Clock.fixed(Instant.from(now), ZoneOffset.UTC);
    }

    private Job jobWithRunAt(String type, String cron, OffsetDateTime runAt) {
        Job job = new Job(UUID.randomUUID(), type, null, 3, 0, null, "__jobq_recurring__:" + cron);
        job.setCron(cron);
        job.setRunAt(runAt);
        return job;
    }
}
