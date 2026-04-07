package com.jobq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        classes = {TestApplication.class, JobQAdvancedIntegrationTest.AdvancedConfig.class},
        properties = {
            "jobq.background-job-server.poll-interval-in-seconds=1",
            "jobq.background-job-server.execution-timeout-check-interval-in-seconds=1",
            "jobq.background-job-server.worker-count=8"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQAdvancedIntegrationTest {

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
    JobRepository jobRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    static volatile CountDownLatch batchLatch;
    static volatile CountDownLatch timedOutAttemptStartedLatch;
    static volatile CountDownLatch allowTimedOutAttemptToFinishLatch;
    static volatile CountDownLatch timeoutRetryCompletedLatch;
    static final AtomicInteger timeoutAttemptCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "TRUNCATE TABLE jobq_job_logs, jobq_dashboard_audit_log, jobq_worker_nodes, jobq_queue_controls, jobq_jobs RESTART IDENTITY CASCADE");
        batchLatch = null;
        timedOutAttemptStartedLatch = null;
        allowTimedOutAttemptToFinishLatch = null;
        timeoutRetryCompletedLatch = null;
        timeoutAttemptCounter.set(0);
    }

    @Configuration
    static class AdvancedConfig {

        @Bean
        BatchWorker batchWorker() {
            return new BatchWorker();
        }

        @Bean
        TimeoutRescueWorker timeoutRescueWorker() {
            return new TimeoutRescueWorker();
        }
    }

    @com.jobq.annotation.Job("BATCH_JOB")
    static class BatchWorker implements JobWorker<TestPayload> {
        @Override
        public void process(UUID jobId, TestPayload payload) {
            if (batchLatch != null) {
                batchLatch.countDown();
            }
        }
    }

    @com.jobq.annotation.Job(
            value = "TIMEOUT_RESCUE_JOB",
            maxRetries = 1,
            maxExecutionMs = 250,
            initialBackoffMs = 25,
            backoffMultiplier = 1.0)
    static class TimeoutRescueWorker implements JobWorker<TestPayload> {
        @Override
        public void process(UUID jobId, TestPayload payload) throws Exception {
            int attempt = timeoutAttemptCounter.incrementAndGet();
            if (attempt == 1) {
                if (timedOutAttemptStartedLatch != null) {
                    timedOutAttemptStartedLatch.countDown();
                }
                if (allowTimedOutAttemptToFinishLatch != null) {
                    allowTimedOutAttemptToFinishLatch.await(5, TimeUnit.SECONDS);
                }
                return;
            }
            if (timeoutRetryCompletedLatch != null) {
                timeoutRetryCompletedLatch.countDown();
            }
        }
    }

    public static class TestPayload {
        private String value;

        public TestPayload() {}

        public TestPayload(String value) {
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
    void shouldBatchEnqueueAndProcessAllJobs() throws InterruptedException {
        int jobs = 120;
        batchLatch = new CountDownLatch(jobs);

        List<TestPayload> payloads = java.util.stream.IntStream.range(0, jobs)
                .mapToObj(i -> new TestPayload("batch-" + i))
                .toList();

        List<UUID> ids = jobClient.enqueueAll(BatchWorker.class, payloads);

        assertEquals(jobs, ids.size());
        assertTrue(batchLatch.await(30, TimeUnit.SECONDS), "Timed out waiting for batch jobs");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Long total =
                    jdbcTemplate.queryForObject("SELECT count(*) FROM jobq_jobs WHERE type = 'BATCH_JOB'", Long.class);
            Long completed = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM jobq_jobs WHERE type = 'BATCH_JOB' AND finished_at IS NOT NULL", Long.class);
            assertEquals(jobs, total == null ? 0L : total.longValue());
            assertEquals(jobs, completed == null ? 0L : completed.longValue());
        });
    }

    @Test
    void shouldFenceTimedOutAttemptAndRetryWithIncrementedRetryCount() throws InterruptedException {
        timedOutAttemptStartedLatch = new CountDownLatch(1);
        allowTimedOutAttemptToFinishLatch = new CountDownLatch(1);
        timeoutRetryCompletedLatch = new CountDownLatch(1);

        UUID jobId = jobClient.enqueue(TimeoutRescueWorker.class, new TestPayload("timeout"));

        assertTrue(timedOutAttemptStartedLatch.await(10, TimeUnit.SECONDS), "First attempt never started");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(1, job.getRetryCount());
            assertNotNull(job.getRunAt());
            assertTrue(job.getFailedAt() == null);
        });

        allowTimedOutAttemptToFinishLatch.countDown();
        assertTrue(timeoutRetryCompletedLatch.await(15, TimeUnit.SECONDS), "Retry attempt never completed");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(1, job.getRetryCount());
            assertNotNull(job.getFinishedAt());
            assertEquals(2, timeoutAttemptCounter.get());
        });
    }
}
