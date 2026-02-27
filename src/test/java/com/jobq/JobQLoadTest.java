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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = { TestApplication.class,
        JobQLoadTest.LoadTestConfig.class }, properties = {
                "jobq.background-job-server.poll-interval-in-seconds=1",
                "jobq.background-job-server.worker-count=32"
        })
@ActiveProfiles("test")
@Testcontainers
@Tag("load")
class JobQLoadTest {

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

    static volatile CountDownLatch fastLatch;
    static volatile CountDownLatch scheduledLatch;
    static final ConcurrentHashMap<UUID, AtomicInteger> flakyAttempts = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        jdbcTemplate.update("TRUNCATE TABLE jobq_jobs");
        fastLatch = null;
        scheduledLatch = null;
        flakyAttempts.clear();
    }

    @Configuration
    static class LoadTestConfig {

        @Bean
        JobWorker<TestPayload> loadFastWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "LOAD_FAST_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    if (fastLatch != null) {
                        fastLatch.countDown();
                    }
                }
            };
        }

        @Bean
        JobWorker<TestPayload> loadScheduledWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "LOAD_SCHEDULED_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    if (scheduledLatch != null) {
                        scheduledLatch.countDown();
                    }
                }
            };
        }

        @Bean
        JobWorker<TestPayload> loadDedupWorker() {
            return new LoadDedupWorker();
        }

        @com.jobq.annotation.Job(value = "LOAD_DEDUP_JOB", initialDelayMs = 60_000)
        static class LoadDedupWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                // no-op. This workload test validates enqueue/dedup pressure while jobs remain active.
            }
        }

        @Bean
        JobWorker<TestPayload> loadFlakyWorker() {
            return new LoadFlakyWorker();
        }

        @com.jobq.annotation.Job(value = "LOAD_FLAKY_JOB", maxRetries = 3, initialBackoffMs = 20, backoffMultiplier = 1.0)
        static class LoadFlakyWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                int attempt = flakyAttempts.computeIfAbsent(jobId, ignored -> new AtomicInteger(0)).incrementAndGet();
                if (attempt < 3) {
                    throw new RuntimeException("transient load error");
                }
            }
        }

        @Bean
        JobWorker<TestPayload> loadExpectedWorker() {
            return new LoadExpectedWorker();
        }

        @com.jobq.annotation.Job(value = "LOAD_EXPECTED_JOB", expectedExceptions = LoadExpectedBusinessException.class)
        static class LoadExpectedWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                throw new LoadExpectedBusinessException("already done");
            }
        }
    }

    static class LoadExpectedBusinessException extends RuntimeException {
        LoadExpectedBusinessException(String message) {
            super(message);
        }
    }

    public static class TestPayload {
        private String message;

        public TestPayload() {
        }

        public TestPayload(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    @Test
    void shouldSustainHighVolumeThroughputWithoutDataLoss() throws InterruptedException {
        int jobs = 1_500;
        fastLatch = new CountDownLatch(jobs);

        long enqueueStart = System.nanoTime();
        runConcurrently(jobs, 12, i -> jobClient.enqueue("LOAD_FAST_JOB", new TestPayload("fast-" + i)));
        long enqueueMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - enqueueStart);

        assertTrue(fastLatch.await(90, TimeUnit.SECONDS), "Timed out waiting for high-volume processing");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(jobs, countByType("LOAD_FAST_JOB"));
            assertEquals(jobs, countCompletedByType("LOAD_FAST_JOB"));
            assertEquals(0, countFailedByType("LOAD_FAST_JOB"));
            assertEquals(0, countActiveByType("LOAD_FAST_JOB"));
        });

        assertTrue(enqueueMillis < 30_000, "Enqueue under load should finish in a reasonable time");
    }

    @Test
    void shouldDeduplicateUnderHighContentionWithoutCreatingExtraRows() throws InterruptedException {
        int keys = 200;
        int calls = 4_000;
        Map<String, UUID> idPerKey = new ConcurrentHashMap<>();
        List<String> mismatches = new CopyOnWriteArrayList<>();

        runConcurrently(calls, 20, i -> {
            String key = "dedup-key-" + ThreadLocalRandom.current().nextInt(keys);
            UUID returnedId = jobClient.enqueue("LOAD_DEDUP_JOB", new TestPayload("payload-" + i), "load", key);
            UUID existing = idPerKey.putIfAbsent(key, returnedId);
            if (existing != null && !existing.equals(returnedId)) {
                mismatches.add(key);
            }
        });

        assertTrue(mismatches.isEmpty(), "Dedup returned different ids for same replaceKey: " + mismatches);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(keys, idPerKey.size());
            assertEquals(keys, countActiveByType("LOAD_DEDUP_JOB"));
            assertEquals(keys, countByType("LOAD_DEDUP_JOB"));
        });
    }

    @Test
    void shouldHandleMixedRetryAndExpectedExceptionLoadConsistently() throws InterruptedException {
        int flakyJobs = 400;
        int expectedJobs = 400;

        runConcurrently(flakyJobs, 10, i -> jobClient.enqueue("LOAD_FLAKY_JOB", new TestPayload("flaky-" + i), 3));
        runConcurrently(expectedJobs, 10,
                i -> jobClient.enqueue("LOAD_EXPECTED_JOB", new TestPayload("expected-" + i), 3));

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertEquals(flakyJobs, countCompletedByType("LOAD_FLAKY_JOB"));
            assertEquals(0, countFailedByType("LOAD_FLAKY_JOB"));
            assertEquals(0, countActiveByType("LOAD_FLAKY_JOB"));

            assertEquals(expectedJobs, countCompletedByType("LOAD_EXPECTED_JOB"));
            assertEquals(0, countFailedByType("LOAD_EXPECTED_JOB"));
            assertEquals(0, countActiveByType("LOAD_EXPECTED_JOB"));
        });

        long flakyRetriedTwice = queryCount(
                "SELECT count(*) FROM jobq_jobs WHERE type = ? AND finished_at IS NOT NULL AND retry_count = 2",
                "LOAD_FLAKY_JOB");
        long expectedNoRetries = queryCount(
                "SELECT count(*) FROM jobq_jobs WHERE type = ? AND finished_at IS NOT NULL AND retry_count = 0",
                "LOAD_EXPECTED_JOB");

        assertEquals(flakyJobs, flakyRetriedTwice);
        assertEquals(expectedJobs, expectedNoRetries);
    }

    @Test
    void shouldRespectScheduledRunAtAtScale() throws InterruptedException {
        int jobs = 500;
        scheduledLatch = new CountDownLatch(jobs);
        OffsetDateTime runAt = OffsetDateTime.now().plusSeconds(3);

        runConcurrently(jobs, 12,
                i -> jobClient.enqueueAt("LOAD_SCHEDULED_JOB", new TestPayload("scheduled-" + i), runAt));

        Thread.sleep(1_500);
        assertEquals(0, countCompletedByType("LOAD_SCHEDULED_JOB"));
        assertEquals(jobs, countActiveByType("LOAD_SCHEDULED_JOB"));

        assertTrue(scheduledLatch.await(90, TimeUnit.SECONDS), "Timed out waiting for scheduled workload");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(jobs, countCompletedByType("LOAD_SCHEDULED_JOB"));
            assertEquals(0, countFailedByType("LOAD_SCHEDULED_JOB"));
            assertEquals(0, countActiveByType("LOAD_SCHEDULED_JOB"));
        });
    }

    private void runConcurrently(int tasks, int threads, IntConsumer action) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(Math.min(tasks, threads));
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(tasks);
        List<Throwable> failures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < tasks; i++) {
            final int index = i;
            executor.execute(() -> {
                ready.countDown();
                try {
                    if (!start.await(15, TimeUnit.SECONDS)) {
                        failures.add(new IllegalStateException("start latch timeout"));
                        return;
                    }
                    action.accept(index);
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(ready.await(30, TimeUnit.SECONDS), "Workers did not become ready");
        start.countDown();
        assertTrue(done.await(180, TimeUnit.SECONDS), "Concurrent workload did not finish in time");
        executor.shutdownNow();
        assertTrue(failures.isEmpty(), "Concurrent workload produced failures: " + failures);
    }

    private long countByType(String type) {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE type = ?", type);
    }

    private long countCompletedByType(String type) {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE type = ? AND finished_at IS NOT NULL", type);
    }

    private long countFailedByType(String type) {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE type = ? AND failed_at IS NOT NULL", type);
    }

    private long countActiveByType(String type) {
        return queryCount("SELECT count(*) FROM jobq_jobs WHERE type = ? AND finished_at IS NULL AND failed_at IS NULL",
                type);
    }

    private long queryCount(String sql, Object... args) {
        Long count = jdbcTemplate.queryForObject(sql, Long.class, args);
        return count == null ? 0L : count;
    }
}
