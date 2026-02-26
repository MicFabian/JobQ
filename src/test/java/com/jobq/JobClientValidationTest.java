package com.jobq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.config.JobQProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

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
    private JobClient jobClient;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        jdbcTemplate = mock(JdbcTemplate.class);

        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setTablePrefix("");

        jobClient = new JobClient(jobRepository, new ObjectMapper(), properties, jdbcTemplate);
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
}
