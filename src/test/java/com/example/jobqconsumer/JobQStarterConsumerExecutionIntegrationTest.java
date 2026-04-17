package com.example.jobqconsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.Job;
import com.jobq.JobClient;
import com.jobq.JobRepository;
import com.jobq.internal.JobOperationsService;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        classes = JobQStarterConsumerExecutionIntegrationTest.ConsumerApplication.class,
        properties = {
            "jobq.dashboard.enabled=false",
            "jobq.background-job-server.poll-interval-in-seconds=1",
            "jobq.background-job-server.notify-enabled=false",
            "spring.jpa.hibernate.ddl-auto=create-drop"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQStarterConsumerExecutionIntegrationTest {

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
    ConsumerNoteRepository consumerNoteRepository;

    @Autowired
    JobOperationsService jobOperationsService;

    @BeforeEach
    void clearState() {
        consumerNoteRepository.deleteAll();
        jobRepository.deleteAll();
    }

    @Test
    void shouldExecuteConsumerWorkerWhenEnqueuedByExplicitStringType() {
        UUID jobId = jobClient.enqueue("CONSUMER_AUTO_CONFIG_JOB", new ConsumerAutoConfigJob.Payload("hello"));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(3, job.getMaxRetries());
            assertEquals(1, consumerNoteRepository.count());
            assertEquals(
                    "explicit:hello", consumerNoteRepository.findAll().get(0).getMessage());
        });
    }

    @Test
    void shouldExecuteConsumerWorkerWhenAnnotationValueIsOmittedAndClassApiIsUsed() {
        UUID jobId = jobClient.enqueue(ConsumerClassNameJob.class, new ConsumerClassNameJob.Payload("world"));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            assertEquals("COMPLETED", job.getStatus());
            assertEquals(ConsumerClassNameJob.class.getSimpleName(), job.getType());
            assertEquals(3, job.getMaxRetries());
            assertEquals(1, consumerNoteRepository.count());
            assertEquals("class:world", consumerNoteRepository.findAll().get(0).getMessage());
        });
    }

    @Test
    void shouldPublishWorkerNodeHeartbeatForAutoRegisteredJobs() {
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var nodes = jobOperationsService.loadWorkerNodes(Duration.ofMinutes(1));
            assertFalse(nodes.isEmpty());
            assertTrue(nodes.stream().anyMatch(node -> node.queueTypes().contains("CONSUMER_AUTO_CONFIG_JOB")));
            assertTrue(nodes.stream()
                    .anyMatch(node -> node.queueTypes().contains(ConsumerClassNameJob.class.getSimpleName())));
        });
    }

    @SpringBootApplication(scanBasePackages = "com.example.jobqconsumer")
    static class ConsumerApplication {}
}
