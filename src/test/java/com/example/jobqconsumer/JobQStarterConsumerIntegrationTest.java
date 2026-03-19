package com.example.jobqconsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.Job;
import com.jobq.JobClient;
import com.jobq.JobRepository;
import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import java.util.UUID;
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
        classes = JobQStarterConsumerIntegrationTest.ConsumerApplication.class,
        properties = {"jobq.background-job-server.enabled=false", "jobq.dashboard.enabled=false"})
@ActiveProfiles("test")
@Testcontainers
class JobQStarterConsumerIntegrationTest {

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
    JobRepository jobRepository;

    @Autowired
    ConsumerNoteRepository consumerNoteRepository;

    @Autowired
    JobClient jobClient;

    @Autowired
    EntityManagerFactory entityManagerFactory;

    @Test
    void shouldAutoRegisterJobRepositoryWithoutConsumerRepositoryScan() {
        assertNotNull(jobRepository);
    }

    @Test
    void shouldKeepConsumerRepositoryRegistrationWorking() {
        assertNotNull(consumerNoteRepository);
    }

    @Test
    void shouldAutoRegisterJobEntityWithoutConsumerEntityScan() {
        assertDoesNotThrow(() -> entityManagerFactory.getMetamodel().entity(Job.class));
    }

    @Test
    void shouldKeepConsumerEntityRegistrationWorking() {
        assertDoesNotThrow(() -> entityManagerFactory.getMetamodel().entity(ConsumerNote.class));
    }

    @Test
    void shouldEnqueueJobsWithoutConsumerSideJpaConfiguration() {
        UUID jobId = jobClient.enqueue("CONSUMER_AUTO_CONFIG_JOB", Map.of("ok", true));
        assertTrue(jobRepository.findById(jobId).isPresent());
    }

    @SpringBootApplication(scanBasePackages = "com.example.jobqconsumer")
    static class ConsumerApplication {}
}
