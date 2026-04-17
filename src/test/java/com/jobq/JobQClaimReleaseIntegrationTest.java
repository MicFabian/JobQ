package com.jobq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.internal.JobPoller;
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
        classes = {TestApplication.class, JobQClaimReleaseIntegrationTest.ClaimReleaseTestConfig.class},
        properties = {
            "jobq.background-job-server.poll-interval-in-seconds=60",
            "jobq.background-job-server.worker-count=4",
            "jobq.background-job-server.virtual-threads-enabled=true",
            "jobq.background-job-server.notify-enabled=false"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQClaimReleaseIntegrationTest {

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
    JobPoller jobPoller;

    @Autowired
    JdbcTemplate jdbcTemplate;

    static volatile CountDownLatch startedLatch;
    static volatile CountDownLatch proceedLatch;

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "TRUNCATE TABLE jobq_job_logs, jobq_dashboard_audit_log, jobq_worker_nodes, jobq_queue_controls, jobq_jobs RESTART IDENTITY CASCADE");
        startedLatch = new CountDownLatch(4);
        proceedLatch = new CountDownLatch(1);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ClaimReleaseTestConfig {

        @Bean
        JobWorker<TestPayload> firstTypeWorker() {
            return new JobWorker<>() {
                @Override
                public String getJobType() {
                    return "CLAIM_RELEASE_A";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) throws InterruptedException {
                    awaitRelease();
                }
            };
        }

        @Bean
        JobWorker<TestPayload> secondTypeWorker() {
            return new JobWorker<>() {
                @Override
                public String getJobType() {
                    return "CLAIM_RELEASE_B";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) throws InterruptedException {
                    awaitRelease();
                }
            };
        }
    }

    private static void awaitRelease() throws InterruptedException {
        startedLatch.countDown();
        if (!proceedLatch.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("blocking worker did not resume");
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
    void shouldRequeueClaimedJobsThatCouldNotBeDispatched() throws InterruptedException {
        int jobsPerType = 8;
        int totalJobs = jobsPerType * 2;

        for (int i = 0; i < jobsPerType; i++) {
            jobClient.enqueue("CLAIM_RELEASE_A", new TestPayload("a-" + i));
            jobClient.enqueue("CLAIM_RELEASE_B", new TestPayload("b-" + i));
        }

        jobPoller.poll();
        assertTrue(startedLatch.await(10, TimeUnit.SECONDS), "expected first dispatch wave to start");

        proceedLatch.countDown();

        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    jobPoller.poll();
                    assertEquals(totalJobs, countCompleted());
                    assertEquals(0, countFailed());
                    assertEquals(0, countActive());
                });
    }

    private long countCompleted() {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE finished_at IS NOT NULL");
    }

    private long countFailed() {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE failed_at IS NOT NULL");
    }

    private long countActive() {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE finished_at IS NULL AND failed_at IS NULL");
    }

    private long queryCount(String sql) {
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count == null ? 0L : count;
    }
}
