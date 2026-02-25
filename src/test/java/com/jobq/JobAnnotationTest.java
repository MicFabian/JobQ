package com.jobq;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

@SpringBootTest(classes = { TestApplication.class,
        JobAnnotationTest.AnnotationTestConfig.class }, properties = "jobq.background-job-server.poll-interval-in-seconds=1")
@ActiveProfiles("test")
@Testcontainers
public class JobAnnotationTest {

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

    static volatile CountDownLatch jobLatch;

    @Configuration
    static class AnnotationTestConfig {

        @com.jobq.annotation.Job(value = "ANNOTATION_BACKOFF_JOB", maxRetries = 3, initialBackoffMs = 500, backoffMultiplier = 2.0)
        static class AnnotationBackoffWorker implements JobWorker<Payload> {
            @Override
            public String getJobType() {
                return "ANNOTATION_BACKOFF_JOB";
            }

            @Override
            public void process(UUID jobId, Payload payload) throws Exception {
                if (jobLatch != null)
                    jobLatch.countDown();
                throw new RuntimeException("Backoff failure #" + payload.getLabel());
            }

            @Override
            public Class<Payload> getPayloadClass() {
                return Payload.class;
            }
        }

        @Bean
        JobWorker<Payload> annotationBackoffWorker() {
            return new AnnotationBackoffWorker();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_HIGHER_JOB", maxRetries = 4, initialBackoffMs = 200, backoffMultiplier = 1.0, retryPriority = com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY)
        static class AnnotationHigherWorker implements JobWorker<Payload> {
            @Override
            public String getJobType() {
                return "ANNOTATION_HIGHER_JOB";
            }

            @Override
            public void process(UUID jobId, Payload payload) throws Exception {
                throw new RuntimeException("Higher failure");
            }

            @Override
            public Class<Payload> getPayloadClass() {
                return Payload.class;
            }
        }

        @Bean
        JobWorker<Payload> annotationHigherWorker() {
            return new AnnotationHigherWorker();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_LOWER_JOB", maxRetries = 4, initialBackoffMs = 200, backoffMultiplier = 1.0, retryPriority = com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY)
        static class AnnotationLowerWorker implements JobWorker<Payload> {
            @Override
            public String getJobType() {
                return "ANNOTATION_LOWER_JOB";
            }

            @Override
            public void process(UUID jobId, Payload payload) throws Exception {
                throw new RuntimeException("Lower failure");
            }

            @Override
            public Class<Payload> getPayloadClass() {
                return Payload.class;
            }
        }

        @Bean
        JobWorker<Payload> annotationLowerWorker() {
            return new AnnotationLowerWorker();
        }

        @com.jobq.annotation.Job(value = "ANNOTATION_NORMAL_JOB", maxRetries = 3, initialBackoffMs = 200, backoffMultiplier = 1.0, retryPriority = com.jobq.annotation.Job.RetryPriority.NORMAL)
        static class AnnotationNormalWorker implements JobWorker<Payload> {
            @Override
            public String getJobType() {
                return "ANNOTATION_NORMAL_JOB";
            }

            @Override
            public void process(UUID jobId, Payload payload) throws Exception {
                throw new RuntimeException("Normal failure");
            }

            @Override
            public Class<Payload> getPayloadClass() {
                return Payload.class;
            }
        }

        @Bean
        JobWorker<Payload> annotationNormalWorker() {
            return new AnnotationNormalWorker();
        }

        @Bean
        JobWorker<Payload> noAnnotationWorker() {
            return new JobWorker<Payload>() {
                @Override
                public String getJobType() {
                    return "NO_ANNOTATION_JOB";
                }

                @Override
                public void process(UUID jobId, Payload payload) throws Exception {
                    if (jobLatch != null)
                        jobLatch.countDown();
                    throw new RuntimeException("No annotation failure");
                }

                @Override
                public Class<Payload> getPayloadClass() {
                    return Payload.class;
                }
            };
        }
    }

    public static class Payload {
        private String label;

        public Payload() {
        }

        public Payload(String label) {
            this.label = label;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }
    }

    @Test
    void shouldPushRunAtForwardByInitialBackoffMsOnFirstRetry() throws Exception {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("ANNOTATION_BACKOFF_JOB", new Payload("first"));

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals(1, job.getRetryCount());
            assertEquals("PENDING", job.getStatus());
            assertTrue(job.getRunAt().isAfter(OffsetDateTime.now().minusSeconds(2)));
        });
    }

    @Test
    void shouldEscalateBackoffDelayExponentiallyAcrossRetries() {
        UUID jobId = jobClient.enqueue("ANNOTATION_BACKOFF_JOB", new Payload("escalate"), 3);

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            return job.getRetryCount() >= 2;
        });

        Job afterTwo = jobRepository.findById(jobId).orElseThrow();
        OffsetDateTime runAt2 = afterTwo.getRunAt();

        assertTrue(afterTwo.getRetryCount() >= 2);
        assertTrue(runAt2.isAfter(afterTwo.getCreatedAt().plusNanos(900_000_000)));
    }

    @Test
    void shouldMarkJobAsFailedAfterExhaustingRetries() {
        UUID jobId = jobClient.enqueue("ANNOTATION_BACKOFF_JOB", new Payload("exhaust"), 3);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("FAILED", job.getStatus());
            assertEquals(4, job.getRetryCount());
            assertTrue(job.getErrorMessage().contains("Backoff failure"));
        });
    }

    @Test
    void shouldAccumulatePriorityIncreaseAcrossRetries() {
        UUID jobId = jobClient.enqueue("ANNOTATION_HIGHER_JOB", new Payload("higher"), 4);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertTrue(job.getRetryCount() >= 3);
            assertTrue(job.getPriority() >= 3);
        });
    }

    @Test
    void shouldAccumulatePriorityDecreaseAcrossRetries() {
        UUID jobId = jobClient.enqueue("ANNOTATION_LOWER_JOB", new Payload("lower"), 4);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertTrue(job.getRetryCount() >= 3);
            assertTrue(job.getPriority() <= -3);
        });
    }

    @Test
    void shouldKeepPriorityUnchanged() {
        UUID jobId = jobClient.enqueue("ANNOTATION_NORMAL_JOB", new Payload("normal"), 3);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertTrue(job.getRetryCount() >= 2);
            assertEquals(0, job.getPriority());
        });
    }

    @Test
    void shouldPersistErrorMessage() throws Exception {
        jobLatch = new CountDownLatch(1);
        UUID jobId = jobClient.enqueue("ANNOTATION_BACKOFF_JOB", new Payload("error-check"), 3);

        jobLatch.await(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("Backoff failure #error-check", job.getErrorMessage());
        });
    }
}
