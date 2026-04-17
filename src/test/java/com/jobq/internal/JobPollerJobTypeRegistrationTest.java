package com.jobq.internal;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.JobRepository;
import com.jobq.JobRuntime;
import com.jobq.config.JobQProperties;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

class JobPollerJobTypeRegistrationTest {

    static class NamespaceOne {
        @com.jobq.annotation.Job(payload = String.class)
        static class DuplicateJob {
            public void process(UUID jobId, String payload) {
                // no-op
            }
        }
    }

    static class NamespaceTwo {
        @com.jobq.annotation.Job(payload = String.class)
        static class DuplicateJob {
            public void process(UUID jobId, String payload) {
                // no-op
            }
        }
    }

    @Test
    void shouldFailFastWhenImplicitSimpleNameCollides() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("duplicateJobOne", NamespaceOne.DuplicateJob.class, NamespaceOne.DuplicateJob::new);
        context.registerBean("duplicateJobTwo", NamespaceTwo.DuplicateJob.class, NamespaceTwo.DuplicateJob::new);
        context.refresh();

        JobQProperties properties = new JobQProperties();
        properties.getBackgroundJobServer().setNotifyEnabled(false);

        JobPoller poller = new JobPoller(
                mock(JobRepository.class),
                List.of(),
                context,
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

        try {
            IllegalStateException error = assertThrows(IllegalStateException.class, poller::init);
            assertTrue(error.getMessage().contains("Duplicate job type 'DuplicateJob'"));
        } finally {
            ReflectionTestUtils.invokeMethod(poller, "shutdownExecutor");
            context.close();
        }
    }
}
