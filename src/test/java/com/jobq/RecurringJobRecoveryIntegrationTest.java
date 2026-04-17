package com.jobq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        classes = {TestApplication.class, RecurringJobRecoveryIntegrationTest.Config.class},
        properties = {
            "jobq.background-job-server.poll-interval-in-seconds=1",
            "jobq.background-job-server.recurring-reconciliation-interval-in-seconds=1"
        })
@ActiveProfiles("test")
@Testcontainers
class RecurringJobRecoveryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test")
            .withTmpFs(Map.of("/var/lib/postgresql/data", "rw"));

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("testcontainers.postgresql.host", postgres::getHost);
        registry.add("testcontainers.postgresql.port", postgres::getFirstMappedPort);
        registry.add("testcontainers.postgresql.database", postgres::getDatabaseName);
        registry.add("testcontainers.postgresql.username", postgres::getUsername);
        registry.add("testcontainers.postgresql.password", postgres::getPassword);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    static final AtomicInteger recurringFailureAttempts = new AtomicInteger();

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "TRUNCATE TABLE jobq_job_logs, jobq_dashboard_audit_log, jobq_worker_nodes, jobq_queue_controls, jobq_jobs RESTART IDENTITY CASCADE");
        recurringFailureAttempts.set(0);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        JobWorker<Void> recurringFailureWorker() {
            return new RecurringFailureWorker();
        }

        @com.jobq.annotation.Job(value = "RECURRING_FAILURE_RECOVERY_JOB", cron = "*/10 * * * * *", maxRetries = 0)
        static class RecurringFailureWorker implements JobWorker<Void> {
            @Override
            public void process(UUID jobId, Void payload) {
                recurringFailureAttempts.incrementAndGet();
                throw new RuntimeException("expected recurring failure");
            }
        }
    }

    @Test
    void shouldRescheduleRecurringJobAfterTerminalFailure() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Integer failedCount = jdbcTemplate.queryForObject(
                    """
                    SELECT count(*) FROM jobq_jobs
                    WHERE type = 'RECURRING_FAILURE_RECOVERY_JOB'
                      AND failed_at IS NOT NULL
                    """,
                    Integer.class);
            Integer activeCount = jdbcTemplate.queryForObject(
                    """
                    SELECT count(*) FROM jobq_jobs
                    WHERE type = 'RECURRING_FAILURE_RECOVERY_JOB'
                      AND cron = '*/10 * * * * *'
                      AND finished_at IS NULL
                      AND failed_at IS NULL
                      AND cancelled_at IS NULL
                    """,
                    Integer.class);

            assertTrue(recurringFailureAttempts.get() >= 1, "Recurring job should have executed and failed");
            assertTrue(failedCount != null && failedCount >= 1, "Recurring job should record at least one failure");
            assertEquals(
                    1,
                    activeCount == null ? 0 : activeCount,
                    "Recurring reconciler should keep exactly one active recurring execution scheduled");
        });
    }
}
