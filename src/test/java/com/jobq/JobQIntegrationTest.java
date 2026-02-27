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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

    @Autowired
    org.springframework.transaction.support.TransactionTemplate transactionTemplate;

    static volatile CountDownLatch jobLatch;
    static volatile String lastProcessedMessage;
    static final AtomicInteger retryCount = new AtomicInteger(0);
    static final ConcurrentLinkedQueue<UUID> processedJobIds = new ConcurrentLinkedQueue<>();
    static volatile UUID slowJobLockedId;
    static volatile CountDownLatch slowJobStartedLatch;
    static volatile CountDownLatch slowJobFinishLatch;
    static final AtomicInteger recurringJobCount = new AtomicInteger(0);
    static volatile CountDownLatch recurringJobLatch;
    static volatile UUID lastAnnotationOnlyProcessedJobId;
    static volatile String lastAnnotationOnlyMessage;
    static volatile CountDownLatch workerOnErrorLatch;
    static volatile UUID lastWorkerOnErrorJobId;
    static volatile String lastWorkerOnErrorMessage;
    static volatile String lastWorkerOnErrorExceptionType;
    static volatile CountDownLatch annotationOnErrorLatch;
    static volatile UUID lastAnnotationOnErrorJobId;
    static volatile String lastAnnotationOnErrorMessage;
    static volatile String lastAnnotationOnErrorExceptionType;
    static volatile CountDownLatch workerOnSuccessLatch;
    static volatile UUID lastWorkerOnSuccessJobId;
    static volatile String lastWorkerOnSuccessMessage;
    static volatile CountDownLatch annotationOnSuccessLatch;
    static volatile UUID lastAnnotationOnSuccessJobId;
    static volatile String lastAnnotationOnSuccessMessage;
    static volatile CountDownLatch annotationAfterLatch;
    static volatile UUID lastAnnotationAfterJobId;
    static volatile String lastAnnotationAfterMessage;
    static volatile CountDownLatch workerAfterLatch;
    static volatile UUID lastWorkerAfterJobId;
    static volatile String lastWorkerAfterMessage;
    static volatile CountDownLatch workerAfterAlwaysLatch;
    static volatile UUID lastWorkerAfterAlwaysJobId;
    static volatile String lastWorkerAfterAlwaysMessage;
    static volatile CountDownLatch annotationAfterAlwaysLatch;
    static volatile UUID lastAnnotationAfterAlwaysJobId;
    static volatile String lastAnnotationAfterAlwaysMessage;

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
        JobWorker<TestPayload> inferredPayloadJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "INFERRED_PAYLOAD_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    lastProcessedMessage = payload.getMessage();
                    jobLatch.countDown();
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
        JobWorker<TestPayload> workerOnErrorJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_ON_ERROR_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    throw new RuntimeException("Worker onError boom");
                }

                @Override
                public void onError(UUID jobId, TestPayload payload, Exception exception) {
                    lastWorkerOnErrorJobId = jobId;
                    lastWorkerOnErrorMessage = payload != null ? payload.getMessage() : null;
                    lastWorkerOnErrorExceptionType = exception.getClass().getName();
                    if (workerOnErrorLatch != null) {
                        workerOnErrorLatch.countDown();
                    }
                }
            };
        }

        @Bean
        JobWorker<TestPayload> workerOnErrorThrowsJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_ON_ERROR_THROWS_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    throw new RuntimeException("Worker callback failure boom");
                }

                @Override
                public void onError(UUID jobId, TestPayload payload, Exception exception) {
                    throw new IllegalStateException("onError callback failed intentionally");
                }
            };
        }

        @Bean
        JobWorker<TestPayload> workerOnSuccessJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_ON_SUCCESS_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    // no-op
                }

                @Override
                public void onSuccess(UUID jobId, TestPayload payload) {
                    lastWorkerOnSuccessJobId = jobId;
                    lastWorkerOnSuccessMessage = payload != null ? payload.getMessage() : null;
                    if (workerOnSuccessLatch != null) {
                        workerOnSuccessLatch.countDown();
                    }
                }
            };
        }

        @Bean
        JobWorker<TestPayload> workerOnSuccessThrowsJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_ON_SUCCESS_THROWS_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    // no-op
                }

                @Override
                public void onSuccess(UUID jobId, TestPayload payload) {
                    throw new IllegalStateException("onSuccess callback failed intentionally");
                }
            };
        }

        @Bean
        JobWorker<TestPayload> workerAfterJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_AFTER_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    // no-op
                }

                @Override
                public void after(UUID jobId, TestPayload payload) {
                    lastWorkerAfterJobId = jobId;
                    lastWorkerAfterMessage = payload != null ? payload.getMessage() : null;
                    if (workerAfterLatch != null) {
                        workerAfterLatch.countDown();
                    }
                }
            };
        }

        @Bean
        JobWorker<TestPayload> workerAfterAlwaysJob() {
            return new JobWorker<TestPayload>() {
                @Override
                public String getJobType() {
                    return "WORKER_AFTER_ALWAYS_JOB";
                }

                @Override
                public void process(UUID jobId, TestPayload payload) {
                    throw new RuntimeException("worker-after-always-failure");
                }

                @Override
                public void after(UUID jobId, TestPayload payload) {
                    lastWorkerAfterAlwaysJobId = jobId;
                    lastWorkerAfterAlwaysMessage = payload != null ? payload.getMessage() : null;
                    if (workerAfterAlwaysLatch != null) {
                        workerAfterAlwaysLatch.countDown();
                    }
                }
            };
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_ONLY_JOB", payload = TestPayload.class)
        static class AnnotationOnlyJob {
            @SuppressWarnings("unused")
            public void process(UUID jobId, TestPayload payload) {
                lastAnnotationOnlyProcessedJobId = jobId;
                lastAnnotationOnlyMessage = payload.getMessage();
                jobLatch.countDown();
            }
        }

        @Bean
        AnnotationOnlyJob annotationOnlyJob() {
            return new AnnotationOnlyJob();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_ON_ERROR_JOB", payload = TestPayload.class)
        static class AnnotationOnErrorJob {
            @SuppressWarnings("unused")
            public void process(UUID jobId, TestPayload payload) {
                throw new RuntimeException("Annotation onError boom");
            }

            @SuppressWarnings("unused")
            public void onError(UUID jobId, TestPayload payload, Exception exception) {
                lastAnnotationOnErrorJobId = jobId;
                lastAnnotationOnErrorMessage = payload != null ? payload.getMessage() : null;
                lastAnnotationOnErrorExceptionType = exception.getClass().getName();
                if (annotationOnErrorLatch != null) {
                    annotationOnErrorLatch.countDown();
                }
            }
        }

        @Bean
        AnnotationOnErrorJob annotationOnErrorJob() {
            return new AnnotationOnErrorJob();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_ON_SUCCESS_JOB", payload = TestPayload.class)
        static class AnnotationOnSuccessJob {
            @SuppressWarnings("unused")
            public void process(UUID jobId, TestPayload payload) {
                // no-op
            }

            @SuppressWarnings("unused")
            public void onSuccess(UUID jobId, TestPayload payload) {
                lastAnnotationOnSuccessJobId = jobId;
                lastAnnotationOnSuccessMessage = payload != null ? payload.getMessage() : null;
                if (annotationOnSuccessLatch != null) {
                    annotationOnSuccessLatch.countDown();
                }
            }
        }

        @Bean
        AnnotationOnSuccessJob annotationOnSuccessJob() {
            return new AnnotationOnSuccessJob();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_AFTER_JOB", payload = TestPayload.class)
        static class AnnotationAfterJob {
            @SuppressWarnings("unused")
            public void process(UUID jobId, TestPayload payload) {
                // no-op
            }

            @SuppressWarnings("unused")
            public void after(UUID jobId, TestPayload payload) {
                lastAnnotationAfterJobId = jobId;
                lastAnnotationAfterMessage = payload != null ? payload.getMessage() : null;
                if (annotationAfterLatch != null) {
                    annotationAfterLatch.countDown();
                }
            }
        }

        @Bean
        AnnotationAfterJob annotationAfterJob() {
            return new AnnotationAfterJob();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_AFTER_ALWAYS_JOB", payload = TestPayload.class)
        static class AnnotationAfterAlwaysJob {
            @SuppressWarnings("unused")
            public void process(UUID jobId, TestPayload payload) {
                throw new RuntimeException("annotation-after-always-failure");
            }

            @SuppressWarnings("unused")
            public void after(UUID jobId, TestPayload payload) {
                lastAnnotationAfterAlwaysJobId = jobId;
                lastAnnotationAfterAlwaysMessage = payload != null ? payload.getMessage() : null;
                if (annotationAfterAlwaysLatch != null) {
                    annotationAfterAlwaysLatch.countDown();
                }
            }
        }

        @Bean
        AnnotationAfterAlwaysJob annotationAfterAlwaysJob() {
            return new AnnotationAfterAlwaysJob();
        }

        @com.jobq.annotation.Job(value = "EXPECTED_EXCEPTION_JOB", expectedExceptions = { ExpectedBusinessException.class })
        static class ExpectedExceptionWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                jobLatch.countDown();
                throw new ExpectedBusinessException("Already handled business condition");
            }

            @Override
            public Class<TestPayload> getPayloadClass() {
                return TestPayload.class;
            }
        }

        @Bean
        JobWorker<TestPayload> expectedExceptionJobWorker() {
            return new ExpectedExceptionWorker();
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

        @com.jobq.annotation.Job(value = "INITIAL_DELAY_JOB", initialDelayMs = 3000)
        static class InitialDelayJobWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                lastProcessedMessage = payload != null ? payload.getMessage() : null;
                if (jobLatch != null) {
                    jobLatch.countDown();
                }
            }
        }

        @Bean
        JobWorker<TestPayload> initialDelayJobWorker() {
            return new InitialDelayJobWorker();
        }

        @com.jobq.annotation.Job(
                value = "DEDUP_KEEP_DELAY_JOB",
                initialDelayMs = 4000,
                deduplicationRunAtPolicy = com.jobq.annotation.Job.DeduplicationRunAtPolicy.KEEP_EXISTING)
        static class DedupKeepDelayJobWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                // no-op
            }
        }

        @Bean
        JobWorker<TestPayload> dedupKeepDelayJobWorker() {
            return new DedupKeepDelayJobWorker();
        }

        @com.jobq.annotation.Job(value = "DEDUP_UPDATE_DELAY_JOB", initialDelayMs = 4000)
        static class DedupUpdateDelayJobWorker implements JobWorker<TestPayload> {
            @Override
            public void process(UUID jobId, TestPayload payload) {
                // no-op
            }
        }

        @Bean
        JobWorker<TestPayload> dedupUpdateDelayJobWorker() {
            return new DedupUpdateDelayJobWorker();
        }

        @com.jobq.annotation.Job(value = "RECURRING_JOB", cron = "*/2 * * * * *")
        static class RecurringWorker implements JobWorker<Void> {
            @Override
            public void process(UUID jobId, Void payload) {
                recurringJobCount.incrementAndGet();
                if (recurringJobLatch != null) {
                    recurringJobLatch.countDown();
                }
            }

            @Override
            public Class<Void> getPayloadClass() {
                return Void.class;
            }
        }

        @Bean
        JobWorker<Void> recurringJobWorker() {
            return new RecurringWorker();
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

    static class ExpectedBusinessException extends RuntimeException {
        ExpectedBusinessException(String message) {
            super(message);
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
                "id", "type", "payload", "created_at", "updated_at",
                "locked_at", "locked_by", "processing_started_at", "finished_at", "failed_at",
                "error_message", "retry_count", "max_retries", "priority",
                "run_at", "group_id", "replace_key", "cron")));
    }

    @Test
    void shouldCreateThePollingIndex() {
        Integer indexCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'jobq_jobs' AND indexname = 'idx_jobq_jobs_polling'",
                Integer.class);
        assertEquals(1, indexCount);
    }

    @Test
    void shouldCreateTheGroupIndex() {
        Integer indexCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'jobq_jobs' AND indexname = 'idx_jobq_jobs_group'",
                Integer.class);
        assertEquals(1, indexCount);
    }

    @Test
    void shouldCreateTheUniqueReplaceKeyIndex() {
        Integer indexCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM pg_indexes " +
                        "WHERE tablename = 'jobq_jobs' " +
                        "AND indexname = 'idx_jobq_jobs_replace_key' " +
                        "AND indexdef LIKE 'CREATE UNIQUE INDEX%'",
                Integer.class);
        assertEquals(1, indexCount);
    }

    @Test
    void shouldRollBackEnqueuedJobWhenOuterTransactionFails() {
        AtomicReference<UUID> jobIdRef = new AtomicReference<>();

        assertThrows(RuntimeException.class, () -> transactionTemplate.executeWithoutResult(status -> {
            jobIdRef.set(jobClient.enqueue("TEST_JOB", new TestPayload("rolled-back")));
            throw new RuntimeException("force rollback");
        }));

        assertNotNull(jobIdRef.get());
        assertTrue(jobRepository.findById(jobIdRef.get()).isEmpty());
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
            assertNotNull(job.getProcessingStartedAt());
            assertNotNull(job.getFinishedAt());
            assertNull(job.getFailedAt());
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
    void shouldEnqueueAndProcessJobWithInferredPayloadType() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        UUID jobId = jobClient.enqueue("INFERRED_PAYLOAD_JOB", new TestPayload("Inferred payload"));

        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("Inferred payload", lastProcessedMessage);
            assertEquals("COMPLETED", job.getStatus());
        });
    }

    @Test
    void shouldEnqueueAndProcessAnnotationOnlyJobWithoutJobWorkerInterface() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastAnnotationOnlyMessage = null;
        lastAnnotationOnlyProcessedJobId = null;
        UUID jobId = jobClient.enqueue("ANNOTATION_ONLY_JOB", new TestPayload("Annotation only"));

        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastAnnotationOnlyProcessedJobId);
            assertEquals("Annotation only", lastAnnotationOnlyMessage);
            assertEquals("COMPLETED", job.getStatus());
        });
    }

    @Test
    void shouldInvokeOnErrorForJobWorkerWhenProcessThrows() throws InterruptedException {
        workerOnErrorLatch = new CountDownLatch(1);
        lastWorkerOnErrorJobId = null;
        lastWorkerOnErrorMessage = null;
        lastWorkerOnErrorExceptionType = null;

        UUID jobId = jobClient.enqueue("WORKER_ON_ERROR_JOB", new TestPayload("worker-on-error"), 0);
        assertTrue(workerOnErrorLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastWorkerOnErrorJobId);
            assertEquals("worker-on-error", lastWorkerOnErrorMessage);
            assertEquals(RuntimeException.class.getName(), lastWorkerOnErrorExceptionType);
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeOnSuccessForJobWorkerWhenProcessSucceeds() throws InterruptedException {
        workerOnSuccessLatch = new CountDownLatch(1);
        lastWorkerOnSuccessJobId = null;
        lastWorkerOnSuccessMessage = null;

        UUID jobId = jobClient.enqueue("WORKER_ON_SUCCESS_JOB", new TestPayload("worker-on-success"));
        assertTrue(workerOnSuccessLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastWorkerOnSuccessJobId);
            assertEquals("worker-on-success", lastWorkerOnSuccessMessage);
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeAfterAliasForJobWorkerWhenProcessSucceeds() throws InterruptedException {
        workerAfterLatch = new CountDownLatch(1);
        lastWorkerAfterJobId = null;
        lastWorkerAfterMessage = null;

        UUID jobId = jobClient.enqueue("WORKER_AFTER_JOB", new TestPayload("worker-after"));
        assertTrue(workerAfterLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastWorkerAfterJobId);
            assertEquals("worker-after", lastWorkerAfterMessage);
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeAfterForJobWorkerWhenProcessFails() throws InterruptedException {
        workerAfterAlwaysLatch = new CountDownLatch(1);
        lastWorkerAfterAlwaysJobId = null;
        lastWorkerAfterAlwaysMessage = null;

        UUID jobId = jobClient.enqueue("WORKER_AFTER_ALWAYS_JOB", new TestPayload("worker-after-always"), 0);
        assertTrue(workerAfterAlwaysLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastWorkerAfterAlwaysJobId);
            assertEquals("worker-after-always", lastWorkerAfterAlwaysMessage);
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeOnErrorForAnnotationOnlyJobWhenProcessThrows() throws InterruptedException {
        annotationOnErrorLatch = new CountDownLatch(1);
        lastAnnotationOnErrorJobId = null;
        lastAnnotationOnErrorMessage = null;
        lastAnnotationOnErrorExceptionType = null;

        UUID jobId = jobClient.enqueue("ANNOTATION_ON_ERROR_JOB", new TestPayload("annotation-on-error"), 0);
        assertTrue(annotationOnErrorLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastAnnotationOnErrorJobId);
            assertEquals("annotation-on-error", lastAnnotationOnErrorMessage);
            assertEquals(RuntimeException.class.getName(), lastAnnotationOnErrorExceptionType);
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeOnSuccessForAnnotationOnlyJobWhenProcessSucceeds() throws InterruptedException {
        annotationOnSuccessLatch = new CountDownLatch(1);
        lastAnnotationOnSuccessJobId = null;
        lastAnnotationOnSuccessMessage = null;

        UUID jobId = jobClient.enqueue("ANNOTATION_ON_SUCCESS_JOB", new TestPayload("annotation-on-success"));
        assertTrue(annotationOnSuccessLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastAnnotationOnSuccessJobId);
            assertEquals("annotation-on-success", lastAnnotationOnSuccessMessage);
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeAfterAliasForAnnotationOnlyJobWhenProcessSucceeds() throws InterruptedException {
        annotationAfterLatch = new CountDownLatch(1);
        lastAnnotationAfterJobId = null;
        lastAnnotationAfterMessage = null;

        UUID jobId = jobClient.enqueue("ANNOTATION_AFTER_JOB", new TestPayload("annotation-after"));
        assertTrue(annotationAfterLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastAnnotationAfterJobId);
            assertEquals("annotation-after", lastAnnotationAfterMessage);
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
        });
    }

    @Test
    void shouldInvokeAfterForAnnotationOnlyJobWhenProcessFails() throws InterruptedException {
        annotationAfterAlwaysLatch = new CountDownLatch(1);
        lastAnnotationAfterAlwaysJobId = null;
        lastAnnotationAfterAlwaysMessage = null;

        UUID jobId = jobClient.enqueue("ANNOTATION_AFTER_ALWAYS_JOB", new TestPayload("annotation-after-always"), 0);
        assertTrue(annotationAfterAlwaysLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(jobId, lastAnnotationAfterAlwaysJobId);
            assertEquals("annotation-after-always", lastAnnotationAfterAlwaysMessage);
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
        });
    }

    @Test
    void shouldContinueFailureHandlingWhenOnErrorCallbackThrows() {
        UUID jobId = jobClient.enqueue("WORKER_ON_ERROR_THROWS_JOB", new TestPayload("callback-throws"), 0);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("FAILED", job.getStatus());
            assertEquals(1, job.getRetryCount());
            assertEquals("Worker callback failure boom", job.getErrorMessage());
        });
    }

    @Test
    void shouldKeepJobCompletedWhenOnSuccessCallbackThrows() {
        UUID jobId = jobClient.enqueue("WORKER_ON_SUCCESS_THROWS_JOB", new TestPayload("callback-throws-success"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
            assertNull(job.getErrorMessage());
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
        jobLatch = new CountDownLatch(1);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("Fail me"), 2);

        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("FAILED", job.getStatus());
            assertEquals(3, job.getRetryCount());
            assertEquals("Intentional Failure for Retries", job.getErrorMessage());
            assertNotNull(job.getFailedAt());
            assertNull(job.getFinishedAt());
            assertNull(job.getLockedAt());
            assertNull(job.getLockedBy());
        });
    }

    @Test
    void shouldIncreaseRetryCountWhenWorkerThrowsDuringExecution() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("boom"), 5);

        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertTrue(job.getRetryCount() >= 1, "Retry count should increase after worker exception");
            assertNotNull(job.getErrorMessage());
            assertTrue(List.of("PENDING", "FAILED").contains(job.getStatus()));
        });
    }

    @Test
    void shouldMarkJobCompletedWhenWhitelistedExceptionIsThrown() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("EXPECTED_EXCEPTION_JOB", new TestPayload("expected"));

        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(0, job.getRetryCount());
            assertNull(job.getErrorMessage());
            assertNotNull(job.getFinishedAt());
            assertNull(job.getFailedAt());
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
            assertNull(job.getLockedAt());
            assertNull(job.getLockedBy());
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
                "INSERT INTO jobq_jobs (id, type, payload, run_at, max_retries, priority) " +
                        "VALUES (?, 'TEST_JOB', '{\"message\":\"future\"}', NOW() + INTERVAL '1 hour', 3, 0)",
                jobId);

        Thread.sleep(2000);
        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals("PENDING", job.getStatus());

        jdbcTemplate.update("DELETE FROM jobq_jobs WHERE id = ?", jobId);
    }

    @Test
    void shouldApplyInitialDelayFromJobAnnotationWhenEnqueueing() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;

        UUID jobId = jobClient.enqueue("INITIAL_DELAY_JOB", new TestPayload("delayed"));
        Job queued = jobRepository.findById(jobId).orElseThrow();
        assertTrue(queued.getRunAt().isAfter(OffsetDateTime.now().plusSeconds(1)));

        Thread.sleep(1200);
        Job stillPending = jobRepository.findById(jobId).orElseThrow();
        assertEquals("PENDING", stillPending.getStatus());
        assertNull(lastProcessedMessage);

        assertTrue(jobLatch.await(12, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job completed = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", completed.getStatus());
            assertEquals("delayed", lastProcessedMessage);
        });
    }

    @Test
    void shouldAllowExplicitRunAtOnEnqueueAndOverrideInitialDelay() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        OffsetDateTime scheduledRunAt = OffsetDateTime.now().plusSeconds(1);

        long startNanos = System.nanoTime();
        UUID jobId = jobClient.enqueueAt("INITIAL_DELAY_JOB", new TestPayload("explicit"), scheduledRunAt);

        assertTrue(jobLatch.await(6, TimeUnit.SECONDS));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        assertTrue(elapsedMs < 3500, "Explicit runAt should override annotation initialDelayMs");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job completed = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", completed.getStatus());
            assertEquals("explicit", lastProcessedMessage);
            long runAtDeltaMs = Math.abs(Duration.between(scheduledRunAt, completed.getRunAt()).toMillis());
            assertTrue(runAtDeltaMs < 1000, "Persisted runAt should stay close to explicit schedule");
        });
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
    void shouldFetchHigherPriorityJobsFirst() {
        UUID lowId = UUID.randomUUID();
        UUID highId = UUID.randomUUID();

        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, run_at, max_retries, priority, updated_at) " +
                        "VALUES (?, 'PRIORITY_ORDER_JOB', '{\"message\":\"low\"}', NOW(), 3, 0, NOW())",
                lowId);
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, run_at, max_retries, priority, updated_at) " +
                        "VALUES (?, 'PRIORITY_ORDER_JOB', '{\"message\":\"high\"}', NOW(), 3, 10, NOW())",
                highId);

        // Verify the repository query correctly orders by priority DESC.
        // PageRequest of 1 simulates fetching the absolute most urgent job first.
        List<Job> nextJobs = transactionTemplate
                .execute(status -> jobRepository.findNextJobsForUpdate("PRIORITY_ORDER_JOB",
                        org.springframework.data.domain.PageRequest.of(0, 1)));

        assertEquals(1, nextJobs.size());
        assertEquals(highId, nextJobs.get(0).getId());
    }

    // ── New Feature Tests: Deduplication via replaceKey ──

    @Test
    void shouldReplaceExistingPendingJobWithSameReplaceKey() {
        UUID firstId = jobClient.enqueue("TEST_JOB", new TestPayload("original"), "group-a", "dedup-key-1");
        UUID secondId = jobClient.enqueue("TEST_JOB", new TestPayload("updated"), "group-a", "dedup-key-1");

        // Same ID returned — the existing job was updated in-place
        assertEquals(firstId, secondId);

        Job job = jobRepository.findById(firstId).orElseThrow();
        assertEquals("PENDING", job.getStatus());
        assertEquals(0, job.getRetryCount());
        assertTrue(job.getPayload().toString().contains("updated"));
    }

    @Test
    void shouldTreatBlankReplaceKeyAsNoDeduplication() {
        UUID firstId = jobClient.enqueue("TEST_JOB", new TestPayload("a"), "group-a", "   ");
        UUID secondId = jobClient.enqueue("TEST_JOB", new TestPayload("b"), "group-a", "");

        assertNotEquals(firstId, secondId);
    }

    @Test
    void shouldNotReplacePendingJobWhenReplaceKeyDiffers() {
        UUID firstId = jobClient.enqueue("TEST_JOB", new TestPayload("a"), "group-a", "key-1");
        UUID secondId = jobClient.enqueue("TEST_JOB", new TestPayload("b"), "group-a", "key-2");

        assertNotEquals(firstId, secondId);
        assertEquals(2, jobRepository.findAllById(List.of(firstId, secondId)).size());
    }

    @Test
    void shouldCreateNewJobWhenExistingIsNotPending() throws InterruptedException {
        // Enqueue and let it process (COMPLETED)
        jobLatch = new CountDownLatch(1);
        lastProcessedMessage = null;
        UUID firstId = jobClient.enqueue("TEST_JOB", new TestPayload("first"), "group-b", "dedup-key-2");
        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertEquals("COMPLETED", jobRepository.findById(firstId).orElseThrow().getStatus()));

        // Now enqueue with same key — should create a new job, not touch the completed
        // one
        UUID secondId = jobClient.enqueue("TEST_JOB", new TestPayload("second"), "group-b", "dedup-key-2");
        assertNotEquals(firstId, secondId);
    }

    @Test
    void shouldDeduplicateConcurrentEnqueueCallsWithSameReplaceKey() throws InterruptedException {
        int callers = 16;
        String type = "DEDUP_RACE_JOB";
        String replaceKey = "race-key-" + UUID.randomUUID();

        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(callers);
        List<UUID> returnedIds = new CopyOnWriteArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < callers; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    if (!start.await(5, TimeUnit.SECONDS)) {
                        failures.add(new IllegalStateException("start latch timeout"));
                        return;
                    }
                    UUID id = jobClient.enqueue(type, new TestPayload("payload-" + idx), "race-group", replaceKey);
                    returnedIds.add(id);
                } catch (Throwable t1) {
                    failures.add(t1);
                } finally {
                    done.countDown();
                }
            });
            t.start();
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), "Concurrent dedup enqueue should not fail: " + failures);
        assertFalse(returnedIds.isEmpty());
        assertEquals(1, new HashSet<>(returnedIds).size(), "All concurrent callers should receive the same job id");

        Integer pendingRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM jobq_jobs " +
                        "WHERE type = ? AND replace_key = ? " +
                        "AND processing_started_at IS NULL AND finished_at IS NULL AND failed_at IS NULL",
                Integer.class, type, replaceKey);
        assertEquals(1, pendingRows);
    }

    @Test
    void shouldDeduplicateAgainstProcessingJobsWithSameReplaceKey() throws InterruptedException {
        String replaceKey = "processing-key-" + UUID.randomUUID();
        slowJobStartedLatch = new CountDownLatch(1);
        slowJobFinishLatch = new CountDownLatch(1);
        jobLatch = new CountDownLatch(1);

        UUID firstId = jobClient.enqueue("SLOW_JOB", new TestPayload("first"), "slow-group", replaceKey);
        assertTrue(slowJobStartedLatch.await(10, TimeUnit.SECONDS));

        Job processingBefore = jobRepository.findById(firstId).orElseThrow();
        assertEquals("PROCESSING", processingBefore.getStatus());
        assertNotNull(processingBefore.getProcessingStartedAt());
        OffsetDateTime processingStartedAt = processingBefore.getProcessingStartedAt();

        UUID secondId = jobClient.enqueue("SLOW_JOB", new TestPayload("second"), "slow-group", replaceKey);
        assertEquals(firstId, secondId);

        Job stillProcessing = jobRepository.findById(firstId).orElseThrow();
        assertEquals("PROCESSING", stillProcessing.getStatus());
        assertEquals(processingStartedAt, stillProcessing.getProcessingStartedAt());

        Integer activeRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM jobq_jobs WHERE type = ? AND replace_key = ? AND finished_at IS NULL AND failed_at IS NULL",
                Integer.class,
                "SLOW_JOB",
                replaceKey);
        assertEquals(1, activeRows);

        slowJobFinishLatch.countDown();
        assertTrue(jobLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    void shouldKeepExistingRunAtWhenDedupPolicyIsKeepExisting() throws InterruptedException {
        String replaceKey = "keep-delay-key-" + UUID.randomUUID();
        UUID firstId = jobClient.enqueue("DEDUP_KEEP_DELAY_JOB", new TestPayload("first"), "dedup-delay", replaceKey);
        Job first = jobRepository.findById(firstId).orElseThrow();
        OffsetDateTime firstRunAt = first.getRunAt();

        Thread.sleep(150);
        UUID secondId = jobClient.enqueue("DEDUP_KEEP_DELAY_JOB", new TestPayload("second"), "dedup-delay", replaceKey);
        assertEquals(firstId, secondId);

        Job replaced = jobRepository.findById(firstId).orElseThrow();
        assertEquals(firstRunAt, replaced.getRunAt());
        assertTrue(replaced.getPayload().toString().contains("second"));
    }

    @Test
    void shouldUpdateRunAtWhenDedupPolicyIsUpdateOnReplace() throws InterruptedException {
        String replaceKey = "update-delay-key-" + UUID.randomUUID();
        UUID firstId = jobClient.enqueue("DEDUP_UPDATE_DELAY_JOB", new TestPayload("first"), "dedup-delay", replaceKey);
        Job first = jobRepository.findById(firstId).orElseThrow();
        OffsetDateTime firstRunAt = first.getRunAt();

        Thread.sleep(150);
        UUID secondId = jobClient.enqueue("DEDUP_UPDATE_DELAY_JOB", new TestPayload("second"), "dedup-delay", replaceKey);
        assertEquals(firstId, secondId);

        Job replaced = jobRepository.findById(firstId).orElseThrow();
        assertTrue(replaced.getRunAt().isAfter(firstRunAt));
        assertTrue(replaced.getPayload().toString().contains("second"));
    }

    // ── New Feature Tests: GroupId ──

    @Test
    void shouldStoreGroupIdOnJob() {
        UUID jobId = jobClient.enqueue("TEST_JOB", new TestPayload("grouped"), "my-group");
        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals("my-group", job.getGroupId());
    }

    @Test
    void shouldProcessMultipleJobsWithSameGroupInParallel() throws InterruptedException {
        int jobCount = 5;
        processedJobIds.clear();
        jobLatch = new CountDownLatch(jobCount);
        List<UUID> ids = new ArrayList<>();

        for (int i = 0; i < jobCount; i++) {
            ids.add(jobClient.enqueue("CONCURRENT_JOB", new TestPayload("group-job-" + i), "parallel-group"));
        }

        jobLatch.await(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (UUID id : ids) {
                assertEquals("COMPLETED", jobRepository.findById(id).orElseThrow().getStatus());
            }
        });
    }

    // ── New Feature Tests: Restart ──

    @Test
    void shouldRestartFailedJob() throws InterruptedException {
        jobLatch = new CountDownLatch(1);
        retryCount.set(0);
        UUID jobId = jobClient.enqueue("RETRY_JOB", new TestPayload("restart-me"), 0);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertEquals("FAILED", jobRepository.findById(jobId).orElseThrow().getStatus()));

        // Manual restart: reset to PENDING
        Job failedJob = jobRepository.findById(jobId).orElseThrow();
        failedJob.setStatus("PENDING");
        failedJob.setRetryCount(0);
        failedJob.setErrorMessage(null);
        failedJob.setRunAt(OffsetDateTime.now());
        failedJob.setUpdatedAt(OffsetDateTime.now());
        jobRepository.save(failedJob);

        // Verify it goes back to processing
        jobLatch = new CountDownLatch(1);
        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            // It will fail again (maxRetries=0), but this proves restart worked
            assertEquals("FAILED", job.getStatus());
            assertTrue(job.getRetryCount() > 0);
        });
    }

    @Test
    void shouldSkipLockedRowsWhenAnotherTransactionAlreadyHoldsLock() throws InterruptedException {
        String type = "SKIP_LOCKED_JOB";
        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();

        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, run_at, max_retries, priority, created_at, updated_at) " +
                        "VALUES (?, ?, '{\"message\":\"first\"}', NOW(), 3, 0, NOW() - INTERVAL '2 seconds', NOW())",
                firstId, type);
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, run_at, max_retries, priority, created_at, updated_at) " +
                        "VALUES (?, ?, '{\"message\":\"second\"}', NOW(), 3, 0, NOW() - INTERVAL '1 seconds', NOW())",
                secondId, type);

        CountDownLatch firstLockAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirstLock = new CountDownLatch(1);
        AtomicReference<UUID> lockedIdByTx1 = new AtomicReference<>();
        AtomicReference<Throwable> tx1Error = new AtomicReference<>();

        Thread tx1 = new Thread(() -> {
            try {
                transactionTemplate.executeWithoutResult(status -> {
                    List<Job> firstBatch = jobRepository.findNextJobsForUpdate(type,
                            org.springframework.data.domain.PageRequest.of(0, 1));
                    if (!firstBatch.isEmpty()) {
                        lockedIdByTx1.set(firstBatch.get(0).getId());
                    }
                    firstLockAcquired.countDown();
                    try {
                        releaseFirstLock.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });
            } catch (Throwable t) {
                tx1Error.set(t);
            }
        });
        tx1.start();

        assertTrue(firstLockAcquired.await(10, TimeUnit.SECONDS));
        assertNotNull(lockedIdByTx1.get(), "First transaction should lock one row");

        CountDownLatch secondDone = new CountDownLatch(1);
        AtomicReference<List<Job>> secondBatchRef = new AtomicReference<>(List.of());
        AtomicReference<Throwable> tx2Error = new AtomicReference<>();

        Thread tx2 = new Thread(() -> {
            try {
                transactionTemplate.executeWithoutResult(status -> secondBatchRef.set(
                        jobRepository.findNextJobsForUpdate(type, org.springframework.data.domain.PageRequest.of(0, 1))));
            } catch (Throwable t) {
                tx2Error.set(t);
            } finally {
                secondDone.countDown();
            }
        });
        tx2.start();

        boolean finishedQuickly = secondDone.await(2, TimeUnit.SECONDS);
        releaseFirstLock.countDown();
        tx1.join();
        tx2.join();

        assertTrue(finishedQuickly, "Second transaction should not block behind locked row (SKIP LOCKED)");
        assertNull(tx1Error.get());
        assertNull(tx2Error.get());
        assertEquals(1, secondBatchRef.get().size());
        assertNotEquals(lockedIdByTx1.get(), secondBatchRef.get().get(0).getId());
    }

    @Test
    void shouldScheduleAndRunRecurringJobs() throws InterruptedException {
        // The RecurringJobInitializer should have already bootstrapped the job
        // since we defined the bean in TestConfig with a cron expression.

        // Wait for at least 2 executions (cron is every 2 seconds)
        recurringJobLatch = new CountDownLatch(2);

        assertTrue(recurringJobLatch.await(15, TimeUnit.SECONDS),
                "Recurring job should have executed at least twice. Actual count: " + recurringJobCount.get());

        // Success means the job was processed, then rescheduled, then processed again.
        assertTrue(recurringJobCount.get() >= 2);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Long recurringRows = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM jobq_jobs WHERE type = 'RECURRING_JOB' AND cron = '*/2 * * * * *'",
                    Long.class);
            Long activeRows = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM jobq_jobs " +
                            "WHERE type = 'RECURRING_JOB' " +
                            "AND cron = '*/2 * * * * *' " +
                            "AND finished_at IS NULL AND failed_at IS NULL",
                    Long.class);

            assertNotNull(recurringRows);
            assertNotNull(activeRows);
            assertTrue(recurringRows >= 2, "Recurring runs should create subsequent persisted job rows");
            assertTrue(activeRows >= 1, "Recurring scheduler should keep at least one active future execution");
        });
    }

    @Test
    void shouldNotMarkCompletedWhenLockOwnerDoesNotMatch() {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, processing_started_at, locked_at, locked_by, max_retries, retry_count, priority, run_at, updated_at) " +
                        "VALUES (?, 'TEST_JOB', '{\"message\":\"locked\"}', NOW(), NOW(), 'node-owner', 3, 0, 0, NOW(), NOW())",
                jobId);

        int updated = transactionTemplate.execute(status -> jobRepository.markCompleted(jobId, OffsetDateTime.now(), "node-other"));
        assertNotNull(updated);
        assertEquals(0, updated);

        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals("PROCESSING", job.getStatus());
        assertNull(job.getFinishedAt());
    }

    @Test
    void shouldNotApplyRetryUpdateWhenExpectedRetryCountDoesNotMatch() {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, processing_started_at, locked_at, locked_by, max_retries, retry_count, priority, run_at, updated_at, error_message) " +
                        "VALUES (?, 'RETRY_JOB', '{\"message\":\"retry\"}', NOW(), NOW(), 'node-owner', 3, 2, 5, NOW(), NOW(), 'old')",
                jobId);

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextRunAt = now.plusSeconds(30);
        int updated = transactionTemplate.execute(status -> jobRepository.markForRetry(
                jobId,
                1,
                3,
                "new-error",
                now,
                nextRunAt,
                4,
                "node-owner"));
        assertNotNull(updated);
        assertEquals(0, updated);

        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals(2, job.getRetryCount());
        assertEquals(5, job.getPriority());
        assertEquals("old", job.getErrorMessage());
    }

    @Test
    void shouldNotApplyRetryUpdateWhenLockOwnerDoesNotMatch() {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, processing_started_at, locked_at, locked_by, max_retries, retry_count, priority, run_at, updated_at, error_message) " +
                        "VALUES (?, 'RETRY_JOB', '{\"message\":\"retry\"}', NOW(), NOW(), 'node-owner', 3, 1, 5, NOW(), NOW(), 'old')",
                jobId);

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime nextRunAt = now.plusSeconds(30);
        int updated = transactionTemplate.execute(status -> jobRepository.markForRetry(
                jobId,
                1,
                2,
                "new-error",
                now,
                nextRunAt,
                4,
                "node-other"));
        assertNotNull(updated);
        assertEquals(0, updated);

        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals(1, job.getRetryCount());
        assertEquals(5, job.getPriority());
        assertEquals("old", job.getErrorMessage());
        assertEquals("PROCESSING", job.getStatus());
    }

    @Test
    void shouldNotMarkFailedTerminalWhenLockOwnerDoesNotMatch() {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO jobq_jobs (id, type, payload, processing_started_at, locked_at, locked_by, max_retries, retry_count, priority, run_at, updated_at, error_message) " +
                        "VALUES (?, 'RETRY_JOB', '{\"message\":\"retry\"}', NOW(), NOW(), 'node-owner', 1, 1, 0, NOW(), NOW(), 'old')",
                jobId);

        OffsetDateTime now = OffsetDateTime.now();
        int updated = transactionTemplate.execute(status -> jobRepository.markFailedTerminal(
                jobId,
                1,
                2,
                "terminal-error",
                now,
                "node-other"));
        assertNotNull(updated);
        assertEquals(0, updated);

        Job job = jobRepository.findById(jobId).orElseThrow();
        assertEquals(1, job.getRetryCount());
        assertEquals("old", job.getErrorMessage());
        assertEquals("PROCESSING", job.getStatus());
        assertNull(job.getFailedAt());
    }
}
