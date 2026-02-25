package com.jobq;

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

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = { TestApplication.class,
        JobQIntegrationTest.TestConfig.class }, properties = "jobq.background-job-server.poll-interval-in-seconds=1")
@ActiveProfiles("test")
@Testcontainers
public class JobQIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

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

    static volatile CountDownLatch jobLatch;
    static volatile String lastProcessedMessage;
    static final AtomicInteger retryCount = new AtomicInteger(0);
    static final ConcurrentLinkedQueue<UUID> processedJobIds = new ConcurrentLinkedQueue<>();
    static volatile UUID slowJobLockedId;
    static volatile CountDownLatch slowJobStartedLatch;
    static volatile CountDownLatch slowJobFinishLatch;

    @Configuration
    static class TestConfig {
        @Bean
        JobWorker<TestPayload> testJobWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "TEST_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    lastProcessedMessage = payload.getMessage();
                    jobLatch.countDown();
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @Bean
        JobWorker<TestPayload> retryJobWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "RETRY_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) throws Exception {
                    retryCount.incrementAndGet();
                    jobLatch.countDown();
                    throw new RuntimeException("Intentional Failure for Retries");
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @Bean
        JobWorker<TestPayload> nullPayloadWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "NULL_PAYLOAD_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    lastProcessedMessage = payload == null ? "__NULL__" : payload.getMessage();
                    jobLatch.countDown();
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @Bean
        JobWorker<TestPayload> concurrentWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "CONCURRENT_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    processedJobIds.add(jobId);
                    jobLatch.countDown();
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @Bean
        JobWorker<TestPayload> priorityOrderWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "PRIORITY_ORDER_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    processedJobIds.add(jobId);
                    jobLatch.countDown();
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @Bean
        JobWorker<TestPayload> slowJobWorker() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "SLOW_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) throws Exception {
                    slowJobLockedId = jobId;
                    slowJobStartedLatch.countDown();
                    slowJobFinishLatch.await(10, TimeUnit.SECONDS);
                    jobLatch.countDown();
                }

                @Override
                public Class<TestPayload> getPayloadClass() {
                    return TestPayload.class;
                }
            };
        }

        @com.jobq.annotation.Job(value = "BACKOFF_JOB", maxRetries = 3, initialBackoffMs = 3000, backoffMultiplier = 2.0)
        static class BackoffJobWorker implements JobWorker<TestPayload> {
            @Override
            public String getJobType() {
                return "BACKOFF_JOB";
            }

            @Override
            public void process(UUID jobId, TestPayload payload) throws Exception {
                jobLatch.countDown();
                throw new RuntimeException("Backoff Failure");
            }

            @Override
            public Class<TestPayload> getPayloadClass() {
                return TestPayload.class;
            }
        }

        @Bean
        JobWorker<TestPayload> backoffJobWorker() {
            return new BackoffJobWorker();
        }

        @com.jobq.annotation.Job(value = "PRIORITY_JOB", maxRetries = 2, retryPriority = com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY)
        static class HigherPriorityJobWorker implements JobWorker<TestPayload> {
            @Override
            public String getJobType() {
                return "PRIORITY_JOB";
            }

            @Override
            public void process(UUID jobId, TestPayload payload) throws Exception {
                jobLatch.countDown();
                throw new RuntimeException("Priority Failure");
            }

            @Override
            public Class<TestPayload> getPayloadClass() {
                return TestPayload.class;
            }
        }

        @Bean
        JobWorker<TestPayload> higherPriorityJobWorker() {
            return new HigherPriorityJobWorker();
        }

        @com.jobq.annotation.Job(value = "LOWER_PRIORITY_JOB", maxRetries = 2, retryPriority = com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY)
        static class LowerPriorityJobWorker implements JobWorker<TestPayload> {
            @Override
            public String getJobType() {
                return "LOWER_PRIORITY_JOB";
            }

            @Override
            public void process(UUID jobId, TestPayload payload) throws Exception {
                jobLatch.countDown();
                throw new RuntimeException("Lower Priority Failure");
            }

            @Override
            public Class<TestPayload> getPayloadClass() {
                return TestPayload.class;
            }
        }

        @Bean
        JobWorker<TestPayload> lowerPriorityJobWorker() {
            return new LowerPriorityJobWorker();
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
    void shouldCreateJobqJobsTableWithAllRequiredColumns() {
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'jobq_jobs' ORDER BY ordinal_position");
        List<String> columnNames = new ArrayList<>();
        for (Map<String, Object> col : columns) {
            columnNames.add((String) col.get("column_name"));
        }
        assertTrue(columnNames.containsAll(List.of(
                "id", "type", "payload", "status", "created_at", "updated_at",
                "locked_at", "locked_by", "error_message", "retry_count",
                "max_retries", "priority", "run_at")));
    }

    @Test
    void shouldCreateThePollingIndex() {
        Integer indexCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'jobq_jobs' AND indexname = 'idx_jobq_jobs_polling'",
                Integer.class);
        assertEquals(1, indexCount);
    }

    @Test
    void shouldEnqueueAndProcessJobSuccessfully() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        UUID jobId = jobClient.enqueue("TEST_JOB", new TestPayload("Hello, World!"));

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("Hello, World!", lastProcessedMessage);
            assertEquals("COMPLETED", job.getStatus());
            assertNotNull(job.getUpdatedAt());
        });
    }

    @Test
    void shouldEnqueueAndProcessJobWithNullPayload() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        UUID jobId = jobClient.enqueue("NULL_PAYLOAD_JOB", null);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("__NULL__", lastProcessedMessage);
            assertEquals("COMPLETED", job.getStatus());
        });
    }

    @Test
    void shouldHandleLargeJsonPayload() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        String largeMessage = "X".repeat(10_000);
        UUID jobId = jobClient.enqueue("TEST_JOB", new TestPayload(largeMessage));

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(largeMessage, lastProcessedMessage);
            assertEquals("COMPLETED", job.getStatus());
        });
    }

    @Test
    void shouldRetryFailedJobsAndMarkAsFailedAfterMaxRetries() throws InterruptedException {
        jobLatch = new CountDownLatch(3);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("Fail me"), 2);

        jobLatch.await(15, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("FAILED", job.getStatus());
            assertEquals(3, job.getRetryCount());
            assertEquals("Intentional Failure for Retries", job.getErrorMessage());
        });
    }

    @Test
    void shouldImmediatelyFailJobWithMaxRetriesZeroAfterFirstError() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("Zero retries"), 0);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldApplyExponentialBackoffBasedOnAnnotation() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("BACKOFF_JOB", new TestPayload("Backoff test"), 3);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("PENDING", job.getStatus());
            assertEquals(1, job.getRetryCount());
            assertTrue(job.getRunAt().isAfter(OffsetDateTime.now().plusSeconds(1)));
        });
    }

    @Test
    void shouldIncreasePriorityOnRetryWithHigherOnRetry() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("PRIORITY_JOB", new TestPayload("Priority up"), 2);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(1, job.getPriority());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldDecreasePriorityOnRetryWithLowerOnRetry() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("LOWER_PRIORITY_JOB", new TestPayload("Priority down"), 2);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(-1, job.getPriority());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldSetLockedByAndLockedAtWhileProcessing() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        slowJobStartedLatch = new CountDownLatch(1);
        slowJobFinishLatch = new CountDownLatch(1);

        UUID jobId = jobClient.enqueue("SLOW_JOB", new TestPayload("Slow"));
        slowJobStartedLatch.await(10, TimeUnit.SECONDS);

        Job midFlight = jobRepository.findById(jobId).orElseThrow();
        assertEquals("PROCESSING", midFlight.getStatus());
        assertNotNull(midFlight.getLockedBy());
        assertTrue(midFlight.getLockedBy().startsWith("node-"));
        assertNotNull(midFlight.getLockedAt());

        slowJobFinishLatch.countDown();
        jobLatch.await(10, TimeUnit.SECONDS);
    }

    @Test
    void shouldProcess10ConcurrentlyEnqueuedJobs() throws InterruptedException {
        int jobCount = 10;
        jobLatch = new CountDownLatch(jobCount);
        List<UUID> enqueuedIds = Collections.synchronizedList(new ArrayList<>());

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 5; i++)
                enqueuedIds.add(jobClient.enqueue("CONCURRENT_JOB", new TestPayload("Job-1-" + i)));
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 5; i++)
                enqueuedIds.add(jobClient.enqueue("CONCURRENT_JOB", new TestPayload("Job-2-" + i)));
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertEquals(10, enqueuedIds.size());
        jobLatch.await(60, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (UUID id : enqueuedIds) {
                assertEquals("COMPLETED", jobRepository.findById(id).orElseThrow().getStatus());
            }
        });
    }

    @Test
    void shouldNotProcessJobWhoseRunAtIsInTheFuture() throws InterruptedException {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, status, run_at, max_retries, priority) " +
                        "VALUES (?, 'TEST_JOB', '{\"message\":\"future\"}', 'PENDING', NOW() + INTERVAL '1 hour', 3, 0)",
                jobId);

        Thread.sleep(2000);
        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals("PENDING", job.getStatus());

        jdbcTemplate.update("DELETE FROM jobq_jobs WHERE id = ?", jobId);
    }

    @Test
    void shouldPersistLongErrorMessages() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("Error test"), 0);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertNotNull(job.getErrorMessage());
            assertTrue(job.getErrorMessage().length() > 0);
        });
    }

    @Test
    void shouldProcessHigherPriorityJobsBeforeLowerPriorityJobs() throws InterruptedException {
        processedJobIds.clear();
        jobLatch = new CountDownLatch(2);
        UUID lowId = UUID.randomUUID();
        UUID highId = UUID.randomUUID();

        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, status, run_at, max_retries, priority, updated_at) " +
                        "VALUES (?, 'PRIORITY_ORDER_JOB', '{\"message\":\"low\"}', 'PENDING', NOW(), 3, 0, NOW())",
                lowId);
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, status, run_at, max_retries, priority, updated_at) " +
                        "VALUES (?, 'PRIORITY_ORDER_JOB', '{\"message\":\"high\"}', 'PENDING', NOW(), 3, 10, NOW())",
                highId);

        jobLatch.await(15, TimeUnit.SECONDS);

        List<UUID> ordered = new ArrayList<>(processedJobIds);
        assertEquals(2, ordered.size());
        assertEquals(highId, ordered.get(0));
    }
}
