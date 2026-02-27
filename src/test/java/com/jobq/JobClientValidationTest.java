package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.config.JobQProperties;
import com.jobq.internal.JobTypeMetadataRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JobClientValidationTest {

    private JobRepository jobRepository;
    private JdbcTemplate jdbcTemplate;
    private JobTypeMetadataRegistry jobTypeMetadataRegistry;
    private JobClient jobClient;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        jdbcTemplate = mock(JdbcTemplate.class);
        jobTypeMetadataRegistry = mock(JobTypeMetadataRegistry.class);

        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setTablePrefix("");

        jobClient = new JobClient(jobRepository, new ObjectMapper(), properties, jdbcTemplate, jobTypeMetadataRegistry);
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void shouldRejectNullOrBlankJobType() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue(null, "payload"));
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue("   ", "payload"));

        verifyNoInteractions(jobRepository);
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void shouldRejectNegativeMaxRetries() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue("TYPE", "payload", -1, null, null));

        verifyNoInteractions(jobRepository);
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void shouldTrimTypeBeforePersisting() {
        UUID jobId = jobClient.enqueue("  TYPE_WITH_SPACES  ", "payload", 3, null, null);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals("TYPE_WITH_SPACES", saved.getType());
        assertEquals(jobId, saved.getId());
    }

    @Test
    void shouldApplyConfiguredInitialDelayWhenNoRunAtIsProvided() {
        when(jobTypeMetadataRegistry.initialDelayMsFor("DELAYED_JOB")).thenReturn(5_000L);

        jobClient.enqueue("DELAYED_JOB", "payload");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        long delayMs = Duration.between(saved.getUpdatedAt(), saved.getRunAt()).toMillis();
        assertEquals("DELAYED_JOB", saved.getType());
        org.junit.jupiter.api.Assertions.assertTrue(delayMs >= 4_500, "Expected initial delay to be applied");
    }

    @Test
    void shouldUseExplicitRunAtWhenProvided() {
        OffsetDateTime requestedRunAt = OffsetDateTime.now().plusMinutes(2).withNano(0);

        jobClient.enqueueAt("SCHEDULED_JOB", "payload", requestedRunAt);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals(requestedRunAt, saved.getRunAt());
    }

    @Test
    void shouldRejectNullRunAtForEnqueueAt() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueueAt("TYPE", "payload", (OffsetDateTime) null));
    }
}
