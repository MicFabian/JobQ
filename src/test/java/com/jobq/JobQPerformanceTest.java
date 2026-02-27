package com.jobq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = { TestApplication.class,
        JobQPerformanceTest.PerformanceTestConfig.class }, properties = {
                "jobq.background-job-server.poll-interval-in-seconds=1",
                "jobq.background-job-server.worker-count=32",
                "logging.level.com.jobq.JobClient=INFO",
                "logging.level.com.jobq.internal.JobPoller=INFO"
        })
@ActiveProfiles("test")
@Testcontainers
@Tag("perf")
class JobQPerformanceTest {

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

    static volatile CountDownLatch perfFastLatch;

    @BeforeEach
    void setUp() {
        jdbcTemplate.update("TRUNCATE TABLE jobq_jobs");
        perfFastLatch = null;
    }

    @Configuration
    static class PerformanceTestConfig {

        @Bean
        JobWorker<TestPayload> perfFastWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "PERF_FAST_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    if (perfFastLatch != null) {
                        perfFastLatch.countDown();
                    }
                }
            };
        }
    }

    public static class TestPayload {
        private String value;

        public TestPayload() {
        }

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
    void shouldReportEnqueueThroughputUnderConcurrency() throws InterruptedException {
        int jobs = 5_000;

        long startNanos = System.nanoTime();
        runConcurrently(jobs, 24, i -> jobClient.enqueueAt(
                "PERF_FAST_JOB",
                new TestPayload("enqueue-only-" + i),
                OffsetDateTime.now().plusMinutes(20)));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        double throughput = jobs / Math.max(1.0, elapsedMs / 1000.0);

        long persisted = queryCount("SELECT count(*) FROM jobq_jobs WHERE type = 'PERF_FAST_JOB'");
        assertEquals(jobs, persisted);
        assertTrue(elapsedMs < 90_000, "Enqueue performance regression: too slow under concurrency");

        System.out.printf("PERF enqueue_throughput jobs=%d elapsed_ms=%d jobs_per_sec=%.2f%n", jobs, elapsedMs,
                throughput);
    }

    @Test
    void shouldReportEndToEndProcessingThroughput() throws InterruptedException {
        int jobs = 3_000;
        perfFastLatch = new CountDownLatch(jobs);

        long startNanos = System.nanoTime();
        runConcurrently(jobs, 24, i -> jobClient.enqueue("PERF_FAST_JOB", new TestPayload("e2e-" + i)));
        assertTrue(perfFastLatch.await(120, TimeUnit.SECONDS), "Timed out waiting for perf workload completion");
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        double throughput = jobs / Math.max(1.0, elapsedMs / 1000.0);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(jobs,
                    queryCount("SELECT count(*) FROM jobq_jobs WHERE type = 'PERF_FAST_JOB' AND finished_at IS NOT NULL"));
            assertEquals(0,
                    queryCount("SELECT count(*) FROM jobq_jobs WHERE type = 'PERF_FAST_JOB' AND finished_at IS NULL AND failed_at IS NULL"));
        });

        assertTrue(elapsedMs < 120_000, "End-to-end processing performance regression");
        System.out.printf("PERF e2e_throughput jobs=%d elapsed_ms=%d jobs_per_sec=%.2f%n", jobs, elapsedMs,
                throughput);
    }

    private void runConcurrently(int tasks, int threads, IntConsumer action) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(Math.min(tasks, threads));
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(tasks);

        for (int i = 0; i < tasks; i++) {
            final int index = i;
            executor.execute(() -> {
                ready.countDown();
                try {
                    if (start.await(20, TimeUnit.SECONDS)) {
                        action.accept(index);
                    }
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(ready.await(30, TimeUnit.SECONDS), "Workers did not become ready");
        start.countDown();
        assertTrue(done.await(180, TimeUnit.SECONDS), "Workload did not finish in time");
        executor.shutdownNow();
    }

    private long queryCount(String sql) {
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count == null ? 0L : count;
    }
}
