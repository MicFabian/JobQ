package com.jobq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.jobq.dashboard.JobDashboardEventBus;
import com.jobq.internal.JobOperationsService;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

class JobRuntimeTest {

    @Test
    void shouldExposeCurrentContextAndRestorePreviousContext() {
        JobRuntime runtime = new JobRuntime(
                mock(JobRepository.class),
                mock(JobOperationsService.class),
                provider(mock(JobDashboardEventBus.class)));
        UUID outerJobId = UUID.randomUUID();
        UUID innerJobId = UUID.randomUUID();
        OffsetDateTime lockedAt = OffsetDateTime.now();

        assertFalse(runtime.isActive());

        try (JobRuntime.JobExecutionHandle ignored = runtime.open(outerJobId, "OuterJob", lockedAt, "node-a")) {
            assertTrue(runtime.isActive());
            assertEquals(outerJobId, runtime.currentJobId());
            assertEquals("OuterJob", runtime.currentJobType());

            try (JobRuntime.JobExecutionHandle nested = runtime.open(innerJobId, "InnerJob", lockedAt, "node-b")) {
                assertEquals(innerJobId, runtime.currentJobId());
                assertEquals("InnerJob", runtime.currentJobType());
            }

            assertEquals(outerJobId, runtime.currentJobId());
            assertEquals("OuterJob", runtime.currentJobType());
        }

        assertFalse(runtime.isActive());
        assertThrows(IllegalStateException.class, runtime::currentJobId);
    }

    @Test
    void shouldClampProgressNormalizeMessageAndPublishRuntimeUpdate() {
        JobRepository jobRepository = mock(JobRepository.class);
        JobOperationsService jobOperationsService = mock(JobOperationsService.class);
        JobDashboardEventBus eventBus = mock(JobDashboardEventBus.class);
        JobRuntime runtime = new JobRuntime(jobRepository, jobOperationsService, provider(eventBus));
        UUID jobId = UUID.randomUUID();
        OffsetDateTime lockedAt = OffsetDateTime.now();

        try (JobRuntime.JobExecutionHandle ignored = runtime.open(jobId, "ProgressJob", lockedAt, "node-a")) {
            runtime.setProgress(140, "  processing records  ");
            runtime.setProgress(-20, "   ");
        }

        verify(jobRepository)
                .updateProgress(
                        eq(jobId),
                        eq(lockedAt),
                        eq("node-a"),
                        eq(100),
                        eq("processing records"),
                        any(OffsetDateTime.class));
        verify(jobRepository)
                .updateProgress(eq(jobId), eq(lockedAt), eq("node-a"), eq(0), eq(null), any(OffsetDateTime.class));
        verify(eventBus, times(2)).publishJobRuntime(jobId);
    }

    @Test
    void shouldAppendLogsAndPublishRuntimeUpdates() {
        JobRepository jobRepository = mock(JobRepository.class);
        JobOperationsService jobOperationsService = mock(JobOperationsService.class);
        JobDashboardEventBus eventBus = mock(JobDashboardEventBus.class);
        JobRuntime runtime = new JobRuntime(jobRepository, jobOperationsService, provider(eventBus));
        UUID jobId = UUID.randomUUID();
        OffsetDateTime lockedAt = OffsetDateTime.now();

        try (JobRuntime.JobExecutionHandle ignored = runtime.open(jobId, "LoggingJob", lockedAt, "node-a")) {
            runtime.logInfo("processed");
            runtime.logWarn("slow path");
            runtime.logError("failed");
        }

        verify(jobOperationsService).appendJobLog(jobId, "INFO", "processed");
        verify(jobOperationsService).appendJobLog(jobId, "WARN", "slow path");
        verify(jobOperationsService).appendJobLog(jobId, "ERROR", "failed");
        verify(eventBus, times(3)).publishJobRuntime(jobId);
        assertDoesNotThrow(() ->
                runtime.open(UUID.randomUUID(), "ReusedJob", lockedAt, "node-b").close());
    }

    @Test
    void shouldRejectRuntimeOperationsOutsideActiveContext() {
        JobRuntime runtime =
                new JobRuntime(mock(JobRepository.class), mock(JobOperationsService.class), provider(null));

        assertThrows(IllegalStateException.class, runtime::currentJobId);
        assertThrows(IllegalStateException.class, runtime::currentJobType);
        assertThrows(IllegalStateException.class, () -> runtime.setProgress(50));
        assertThrows(IllegalStateException.class, () -> runtime.logInfo("message"));
    }

    private static <T> ObjectProvider<T> provider(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject(Object... args) {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public T getIfUnique() {
                return value;
            }

            @Override
            public T getObject() {
                return value;
            }
        };
    }
}
