package com.jobq.internal;

import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.JobWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    void shouldBootstrapRecurringJobWhenNoActiveExecutionExists() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(
                "RECURRING_BOOTSTRAP_JOB",
                "*/5 * * * * *")).thenReturn(false);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(new RecurringWorker()),
                beanFactory);
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
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(
                "RECURRING_BOOTSTRAP_JOB",
                "*/5 * * * * *")).thenReturn(true);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(new RecurringWorker()),
                beanFactory);
        initializer.start();

        verify(jobRepository, never()).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldIgnoreInvalidCronExpressionsWithoutCrashingStartup() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(
                "INVALID_CRON_JOB",
                "not-a-cron")).thenReturn(false);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(new InvalidCronWorker()),
                beanFactory);

        assertDoesNotThrow(initializer::start);
        verify(jobRepository, never()).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldDetectRecurringAnnotationOnProxiedWorker() {
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(
                "RECURRING_BOOTSTRAP_JOB",
                "*/5 * * * * *")).thenReturn(false);

        ProxyFactory proxyFactory = new ProxyFactory(new RecurringWorker());
        proxyFactory.setProxyTargetClass(true);
        @SuppressWarnings("unchecked")
        JobWorker<Void> proxiedWorker = (JobWorker<Void>) proxyFactory.getProxy();

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(proxiedWorker),
                beanFactory);
        initializer.start();

        verify(jobRepository).save(org.mockito.ArgumentMatchers.any(Job.class));
    }

    @Test
    void shouldBootstrapRecurringJobFromAnnotationOnlyBean() {
        when(beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class))
                .thenReturn(Map.of("annotationOnlyRecurringJob", new AnnotationOnlyRecurringJob()));
        when(jobRepository.existsByTypeAndCronAndFinishedAtIsNullAndFailedAtIsNull(
                "ANNOTATION_ONLY_RECURRING_JOB",
                "*/5 * * * * *")).thenReturn(false);

        RecurringJobInitializer initializer = new RecurringJobInitializer(jobRepository, List.of(), beanFactory);
        initializer.start();

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        assertEquals("ANNOTATION_ONLY_RECURRING_JOB", captor.getValue().getType());
        assertEquals(4, captor.getValue().getMaxRetries());
    }
}
