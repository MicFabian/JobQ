package com.jobq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
        classes = {TestApplication.class, JobQNotificationIntegrationTest.NotificationConfig.class},
        properties = {
            "jobq.background-job-server.poll-interval-in-seconds=60",
            "jobq.background-job-server.notify-enabled=true",
            "jobq.background-job-server.notify-listen-timeout-ms=250",
            "jobq.background-job-server.worker-count=4"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQNotificationIntegrationTest {

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
    JobClient jobClient;

    @Autowired
    JdbcTemplate jdbcTemplate;

    static volatile CountDownLatch notificationLatch;

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "TRUNCATE TABLE jobq_job_logs, jobq_dashboard_audit_log, jobq_worker_nodes, jobq_queue_controls, jobq_jobs RESTART IDENTITY CASCADE");
        notificationLatch = new CountDownLatch(1);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class NotificationConfig {
        @Bean
        NotifiedWorker notifiedWorker() {
            return new NotifiedWorker();
        }
    }

    @com.jobq.annotation.Job("NOTIFIED_JOB")
    static class NotifiedWorker implements JobWorker<NotificationPayload> {
        @Override
        public void process(UUID jobId, NotificationPayload payload) {
            if (notificationLatch != null) {
                notificationLatch.countDown();
            }
        }
    }

    public static class NotificationPayload {
        private String value;

        public NotificationPayload() {}

        public NotificationPayload(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @Test
    void shouldProcessImmediateJobViaPostgresNotificationWithoutWaitingForLongPollInterval() throws Exception {
        jobClient.enqueue(NotifiedWorker.class, new NotificationPayload("hello"));

        assertTrue(notificationLatch.await(8, TimeUnit.SECONDS), "Notification-based wakeup did not process the job");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Long completed = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM jobq_jobs WHERE type = 'NOTIFIED_JOB' AND finished_at IS NOT NULL",
                    Long.class);
            assertEquals(1L, completed == null ? 0L : completed.longValue());
        });
    }
}
