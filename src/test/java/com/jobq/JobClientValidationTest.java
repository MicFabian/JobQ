package com.jobq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.config.JobQProperties;
import com.jobq.internal.JobTypeMetadataRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;

class JobClientValidationTest {

    private JobRepository jobRepository;
    private JdbcTemplate jdbcTemplate;
    private JobTypeMetadataRegistry jobTypeMetadataRegistry;
    private JobClient jobClient;

    @com.jobq.annotation.Job("CLASS_BASED_JOB")
    static class ClassBasedJob {}

    @com.jobq.annotation.Job
    static class ClassNameFallbackJob {}

    static class NonJobClass {}

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        jdbcTemplate = mock(JdbcTemplate.class);
        jobTypeMetadataRegistry = mock(JobTypeMetadataRegistry.class);

        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setTablePrefix("");

        jobClient = new JobClient(jobRepository, new ObjectMapper(), properties, jdbcTemplate, jobTypeMetadataRegistry);
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(jobTypeMetadataRegistry.defaultMaxRetriesFor(anyString(), anyInt()))
                .thenAnswer(invocation -> invocation.getArgument(1));
        when(jobTypeMetadataRegistry.jobTypeFor(any())).thenAnswer(invocation -> {
            Class<?> jobClass = invocation.getArgument(0);
            if (jobClass == null) {
                throw new IllegalArgumentException("Job class must not be null");
            }
            com.jobq.annotation.Job annotation = jobClass.getAnnotation(com.jobq.annotation.Job.class);
            if (annotation != null) {
                String configuredType =
                        annotation.value() == null ? "" : annotation.value().trim();
                return configuredType.isEmpty() ? jobClass.getName() : configuredType;
            }
            throw new IllegalArgumentException("No job type mapping for " + jobClass.getName());
        });
    }

    @Test
    void shouldRejectNullOrBlankJobType() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue((String) null, "payload"));
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue("   ", "payload"));

        verifyNoInteractions(jobRepository);
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void shouldRejectNonJobClassWhenEnqueueingByClass() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue(NonJobClass.class, "payload"));
    }

    @Test
    void shouldRejectNullJobClassWhenEnqueueingByClass() {
        assertThrows(IllegalArgumentException.class, () -> jobClient.enqueue((Class<?>) null, "payload"));
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
    void shouldUseAnnotationMaxRetriesForDefaultEnqueue() {
        when(jobTypeMetadataRegistry.defaultMaxRetriesFor("ANNOTATED_JOB", 10)).thenReturn(3);

        jobClient.enqueue("ANNOTATED_JOB", "payload");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals("ANNOTATED_JOB", saved.getType());
        assertEquals(3, saved.getMaxRetries());
    }

    @Test
    void shouldFallbackToGlobalDefaultRetriesWhenNoAnnotationMaxRetriesExists() {
        jobClient.enqueue("PLAIN_JOB", "payload");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals("PLAIN_JOB", saved.getType());
        assertEquals(10, saved.getMaxRetries());
    }

    @Test
    void shouldResolveTypeFromJobClassWhenEnqueueingByClass() {
        jobClient.enqueue(ClassBasedJob.class, "payload");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals("CLASS_BASED_JOB", saved.getType());
    }

    @Test
    void shouldResolveTypeFromJobClassWhenEnqueueingAtByClassAndInstant() {
        Instant requestedRunAt = Instant.now().plusSeconds(90);

        jobClient.enqueueAt(ClassBasedJob.class, "payload", requestedRunAt);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals("CLASS_BASED_JOB", saved.getType());
        assertEquals(OffsetDateTime.ofInstant(requestedRunAt, ZoneOffset.UTC), saved.getRunAt());
    }

    @Test
    void shouldFallbackToClassNameWhenJobAnnotationValueIsOmitted() {
        jobClient.enqueue(ClassNameFallbackJob.class, "payload");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();

        assertEquals(ClassNameFallbackJob.class.getName(), saved.getType());
    }

    @Test
    void shouldExposeDefaultValueForJobAnnotationType() throws NoSuchMethodException {
        Object defaultValue = com.jobq.annotation.Job.class.getMethod("value").getDefaultValue();
        assertEquals("", defaultValue);
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
        assertThrows(
                IllegalArgumentException.class, () -> jobClient.enqueueAt("TYPE", "payload", (OffsetDateTime) null));
    }

    @Test
    void shouldBatchEnqueueUsingResolvedClassType() {
        when(jdbcTemplate.batchUpdate(
                        anyString(),
                        org.mockito.ArgumentMatchers.<Collection<Object>>any(),
                        anyInt(),
                        org.mockito.ArgumentMatchers.<ParameterizedPreparedStatementSetter<Object>>any()))
                .thenReturn(new int[][] {{1, 1, 1}});

        jobClient.enqueueAllAt(
                ClassBasedJob.class, List.of("a", "b", "c"), Instant.now().plusSeconds(60));

        verify(jdbcTemplate)
                .batchUpdate(
                        anyString(),
                        org.mockito.ArgumentMatchers.<Collection<Object>>any(),
                        eq(3),
                        org.mockito.ArgumentMatchers.<ParameterizedPreparedStatementSetter<Object>>any());
    }

    @Test
    void shouldReturnEmptyListForEmptyBatchEnqueue() {
        List<UUID> ids = jobClient.enqueueAll("TYPE", List.of());

        assertTrue(ids.isEmpty());
        verify(jdbcTemplate, never())
                .batchUpdate(anyString(), org.mockito.ArgumentMatchers.<Collection<Object>>any(), anyInt(), any());
    }

    @Test
    void shouldSynchronizeGroupedRunAtWhenPolicyIsSyncWithNewDelay() {
        OffsetDateTime runAt = OffsetDateTime.now().plusMinutes(5).withNano(0);
        when(jobTypeMetadataRegistry.groupDelayPolicyFor("GROUP_SYNC_JOB"))
                .thenReturn(com.jobq.annotation.Job.GroupDelayPolicy.SYNC_WITH_NEW_DELAY);
        when(jdbcTemplate.update(anyString(), any(PreparedStatementSetter.class)))
                .thenReturn(2);

        jobClient.enqueueAt("GROUP_SYNC_JOB", "payload", 3, "group-a", null, runAt);

        verify(jdbcTemplate).update(anyString(), any(PreparedStatementSetter.class));
    }

    @Test
    void shouldNotSynchronizeGroupedRunAtWhenPolicyKeepsExistingDelay() {
        OffsetDateTime runAt = OffsetDateTime.now().plusMinutes(5).withNano(0);
        when(jobTypeMetadataRegistry.groupDelayPolicyFor("GROUP_KEEP_JOB"))
                .thenReturn(com.jobq.annotation.Job.GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE);

        jobClient.enqueueAt("GROUP_KEEP_JOB", "payload", 3, "group-a", null, runAt);

        verify(jdbcTemplate, never()).update(anyString(), any(PreparedStatementSetter.class));
    }
}
