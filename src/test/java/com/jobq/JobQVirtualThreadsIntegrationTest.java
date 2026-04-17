package com.jobq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        classes = {TestApplication.class, JobQVirtualThreadsIntegrationTest.Config.class},
        properties = {
            "jobq.background-job-server.poll-interval-in-seconds=1",
            "jobq.background-job-server.worker-count=1",
            "jobq.background-job-server.virtual-threads-enabled=true",
            "jobq.background-job-server.notify-enabled=false"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQVirtualThreadsIntegrationTest {

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

    static volatile CountDownLatch blockingJobStartedLatch;
    static volatile CountDownLatch blockingJobReleaseLatch;
    static volatile CountDownLatch blockingJobCompletedLatch;
    static final AtomicBoolean executedOnVirtualThread = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "TRUNCATE TABLE jobq_job_logs, jobq_dashboard_audit_log, jobq_worker_nodes, jobq_queue_controls, jobq_jobs RESTART IDENTITY CASCADE");
        blockingJobStartedLatch = new CountDownLatch(1);
        blockingJobReleaseLatch = new CountDownLatch(1);
        blockingJobCompletedLatch = new CountDownLatch(2);
        executedOnVirtualThread.set(false);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        BlockingVirtualWorker blockingVirtualWorker() {
            return new BlockingVirtualWorker();
        }
    }

    @com.jobq.annotation.Job("VIRTUAL_BOUND_JOB")
    static class BlockingVirtualWorker implements JobWorker<VirtualPayload> {
        @Override
        public void process(UUID jobId, VirtualPayload payload) throws InterruptedException {
            executedOnVirtualThread.set(Thread.currentThread().isVirtual());
            if (blockingJobStartedLatch != null) {
                blockingJobStartedLatch.countDown();
            }
            if (blockingJobReleaseLatch != null) {
                blockingJobReleaseLatch.await(10, TimeUnit.SECONDS);
            }
            if (blockingJobCompletedLatch != null) {
                blockingJobCompletedLatch.countDown();
            }
        }
    }

    static class VirtualPayload {
        private String value;

        public VirtualPayload() {}

        VirtualPayload(String value) {
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
    void shouldRespectWorkerCountWhileUsingVirtualThreads() throws Exception {
        jobClient.enqueue(BlockingVirtualWorker.class, new VirtualPayload("first"));
        jobClient.enqueue(BlockingVirtualWorker.class, new VirtualPayload("second"));

        assertTrue(blockingJobStartedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(executedOnVirtualThread.get(), "Job should execute on a virtual thread when enabled");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer processingCount = jdbcTemplate.queryForObject(
                    """
                    SELECT count(*) FROM jobq_jobs
                    WHERE type = 'VIRTUAL_BOUND_JOB'
                      AND processing_started_at IS NOT NULL
                      AND finished_at IS NULL
                      AND failed_at IS NULL
                      AND cancelled_at IS NULL
                    """,
                    Integer.class);
            Integer pendingCount = jdbcTemplate.queryForObject(
                    """
                    SELECT count(*) FROM jobq_jobs
                    WHERE type = 'VIRTUAL_BOUND_JOB'
                      AND processing_started_at IS NULL
                      AND finished_at IS NULL
                      AND failed_at IS NULL
                      AND cancelled_at IS NULL
                    """,
                    Integer.class);

            assertEquals(1, processingCount == null ? 0 : processingCount);
            assertEquals(1, pendingCount == null ? 0 : pendingCount);
        });

        blockingJobReleaseLatch.countDown();

        assertTrue(blockingJobCompletedLatch.await(15, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer completedCount = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM jobq_jobs WHERE type = 'VIRTUAL_BOUND_JOB' AND finished_at IS NOT NULL",
                    Integer.class);
            assertEquals(2, completedCount == null ? 0 : completedCount);
        });
    }
}
