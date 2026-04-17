package com.jobq.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.JobRepository;
import com.jobq.JobRuntime;
import com.jobq.config.JobQProperties;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

class JobPollerVirtualThreadsTest {

    @Test
    void shouldUsePlatformThreadsByDefault() throws InterruptedException {
        JobPoller poller = newPoller(false);
        try {
            assertFalse(runProcessingTaskAndCaptureVirtualFlag(poller));

            ThreadPoolExecutor pollingExecutor =
                    (ThreadPoolExecutor) ReflectionTestUtils.getField(poller, "pollingExecutor");
            assertFalse(pollingExecutor.getThreadFactory().newThread(() -> {}).isVirtual());

            Thread listenerThread = ReflectionTestUtils.invokeMethod(
                    poller, "createManagedThread", "jobq-listener-test", (Runnable) () -> {}, true);
            assertFalse(listenerThread.isVirtual());
        } finally {
            ReflectionTestUtils.invokeMethod(poller, "shutdownExecutor");
        }
    }

    @Test
    void shouldUseVirtualThreadsOnlyForProcessingWhenEnabled() throws InterruptedException {
        JobPoller poller = newPoller(true);
        try {
            assertTrue(runProcessingTaskAndCaptureVirtualFlag(poller));

            ThreadPoolExecutor pollingExecutor =
                    (ThreadPoolExecutor) ReflectionTestUtils.getField(poller, "pollingExecutor");
            assertFalse(pollingExecutor.getThreadFactory().newThread(() -> {}).isVirtual());

            Thread listenerThread = ReflectionTestUtils.invokeMethod(
                    poller, "createManagedThread", "jobq-listener-test", (Runnable) () -> {}, true);
            assertFalse(listenerThread.isVirtual());
        } finally {
            ReflectionTestUtils.invokeMethod(poller, "shutdownExecutor");
        }
    }

    @Test
    void shouldThrottleNotificationListenerWarnings() {
        JobPoller poller = newPoller(false);
        try {
            assertTrue(poller.shouldWarnNotificationListenerFailure(false, 31_000L));
            assertFalse(poller.shouldWarnNotificationListenerFailure(false, 31_500L));
            assertTrue(poller.shouldWarnNotificationListenerFailure(false, 61_500L));
            assertTrue(poller.shouldWarnNotificationListenerFailure(true, 61_501L));
            assertEquals(
                    "IllegalStateException: boom",
                    poller.summarizeException(new RuntimeException("wrapper", new IllegalStateException("boom"))));
        } finally {
            ReflectionTestUtils.invokeMethod(poller, "shutdownExecutor");
        }
    }

    private boolean runProcessingTaskAndCaptureVirtualFlag(JobPoller poller) throws InterruptedException {
        @SuppressWarnings("unchecked")
        ExecutorService processingExecutor =
                (ExecutorService) ReflectionTestUtils.getField(poller, "processingExecutor");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Boolean> virtualFlag = new AtomicReference<>(Boolean.FALSE);

        processingExecutor.execute(() -> {
            virtualFlag.set(Thread.currentThread().isVirtual());
            latch.countDown();
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "processing executor task should complete");
        return Boolean.TRUE.equals(virtualFlag.get());
    }

    private JobPoller newPoller(boolean virtualThreadsEnabled) {
        JobQProperties properties = new JobQProperties();
        properties.getBackgroundJobServer().setVirtualThreadsEnabled(virtualThreadsEnabled);

        return new JobPoller(
                mock(JobRepository.class),
                List.of(),
                mock(ListableBeanFactory.class),
                new ObjectMapper(),
                mock(TransactionTemplate.class),
                properties,
                mock(DataSource.class),
                mock(JdbcTemplate.class),
                mock(JobSignalPublisher.class),
                mock(JobOperationsService.class),
                mock(JobRuntime.class),
                new ObjectProvider<>() {
                    @Override
                    public com.jobq.dashboard.JobDashboardEventBus getObject(Object... args) {
                        return null;
                    }

                    @Override
                    public com.jobq.dashboard.JobDashboardEventBus getIfAvailable() {
                        return null;
                    }

                    @Override
                    public com.jobq.dashboard.JobDashboardEventBus getIfUnique() {
                        return null;
                    }

                    @Override
                    public com.jobq.dashboard.JobDashboardEventBus getObject() {
                        return null;
                    }
                });
    }
}
