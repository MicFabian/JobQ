package com.jobq.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobQHtmxControllerTest {

    private MockMvc mockMvc;
    private JobRepository jobRepository;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        objectMapper = new ObjectMapper();

        JobQDashboardController controller = new JobQDashboardController(jobRepository, objectMapper);

        this.mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void shouldReturnHtmlForStats() throws Exception {
        JobRepository.LifecycleCounts counts = new JobRepository.LifecycleCounts() {
            @Override
            public Long getPendingCount() {
                return 1L;
            }

            @Override
            public Long getProcessingCount() {
                return 0L;
            }

            @Override
            public Long getCompletedCount() {
                return 0L;
            }

            @Override
            public Long getFailedCount() {
                return 0L;
            }
        };
        when(jobRepository.countLifecycleCounts()).thenReturn(counts);

        mockMvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Total Jobs")))
                .andExpect(content().string(containsString("Pending")))
                .andExpect(content().string(containsString("jobq-refresh")));

        verify(jobRepository, never()).findAllJobs(any(Pageable.class));
    }

    @Test
    void shouldReturnHtmlForJobsTable() throws Exception {
        JobRepository.DashboardJobView job = dashboardRow(
                UUID.randomUUID(),
                "com.example.TestJob",
                OffsetDateTime.now().minusMinutes(3),
                OffsetDateTime.now().minusMinutes(3),
                1,
                3,
                0,
                null,
                null,
                null,
                null,
                null,
                null);

        Page<JobRepository.DashboardJobView> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", containsString("jobqPagination")))
                .andExpect(content().string(containsString("com.example.TestJob")))
                .andExpect(content().string(containsString("1 / 3")))
                .andExpect(content().string(containsString("Retries used: 1")))
                .andExpect(content().string(not(containsString("hx-swap-oob"))));
    }

    @Test
    void shouldRenderRetryButtonAndFailedInfoForFailedJobs() throws Exception {
        JobRepository.DashboardJobView job = dashboardRow(
                UUID.randomUUID(),
                "com.example.FailingJob",
                OffsetDateTime.now().minusMinutes(10),
                OffsetDateTime.now().minusMinutes(10),
                2,
                3,
                0,
                null,
                null,
                null,
                null,
                OffsetDateTime.now().minusMinutes(2),
                "Database timeout while writing status row");

        Page<JobRepository.DashboardJobView> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "FAILED"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Retry")))
                .andExpect(content().string(containsString("Failed at")))
                .andExpect(content().string(containsString("Database timeout while writing status row")));
    }

    @Test
    void shouldReturnHtmlForPaginationControls() throws Exception {
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/pagination"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Page 1")))
                .andExpect(content().string(containsString("jobq-refresh")));
    }

    @Test
    void shouldClampInvalidPageAndSizeValues() throws Exception {
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/jobs").param("page", "-7").param("size", "100000"))
                .andExpect(status().isOk());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(),
                pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getPageNumber());
        assertEquals(200, pageable.getPageSize());
        assertTrue(pageable.getSort().isSorted());
    }

    @Test
    void shouldApplySortingAndFilterFlagsToDashboardQuery() throws Exception {
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/jobs")
                        .param("status", "PROCESSING")
                        .param("query", "sync-customer")
                        .param("sort", "priority-desc")
                        .param("scheduledOnly", "true")
                        .param("retriedOnly", "true"))
                .andExpect(status().isOk());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findDashboardJobViews(
                eq("PROCESSING"),
                eq("sync-customer"),
                eq(true),
                eq(true),
                eq(false),
                eq(new UUID(0L, 0L)),
                pageableCaptor.capture());

        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getPageNumber());
        assertEquals(50, pageable.getPageSize());
        assertNotNull(pageable.getSort().getOrderFor("priority"));
        assertTrue(pageable.getSort().getOrderFor("priority").isDescending());
    }

    @Test
    void shouldSupportScheduledOnlyFilterAndRunAtSorting() throws Exception {
        JobRepository.DashboardJobView delayedJob = dashboardRow(
                UUID.randomUUID(),
                "com.example.DelayedJob",
                OffsetDateTime.now().minusMinutes(1),
                OffsetDateTime.now().plusMinutes(5),
                0,
                3,
                0,
                null,
                null,
                null,
                null,
                null,
                null);
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(delayedJob)));

        mockMvc.perform(get("/jobq/htmx/jobs")
                        .param("scheduledOnly", "true")
                        .param("sort", "run-at-asc"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Runs ")));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findDashboardJobViews(
                eq(""),
                eq(""),
                eq(true),
                eq(false),
                eq(false),
                eq(new UUID(0L, 0L)),
                pageableCaptor.capture());

        Pageable pageable = pageableCaptor.getValue();
        assertNotNull(pageable.getSort().getOrderFor("runAt"));
        assertTrue(pageable.getSort().getOrderFor("runAt").isAscending());
    }

    @Test
    void shouldTreatUuidSearchAsExactIdMatch() throws Exception {
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());
        UUID searchedId = UUID.randomUUID();

        mockMvc.perform(get("/jobq/htmx/jobs")
                        .param("query", searchedId.toString()))
                .andExpect(status().isOk());

        verify(jobRepository).findDashboardJobViews(
                eq(""),
                eq(searchedId.toString()),
                eq(false),
                eq(false),
                eq(true),
                eq(searchedId),
                any(Pageable.class));
    }

    @Test
    void shouldIgnoreUnknownStatusFilterValue() throws Exception {
        JobRepository.DashboardJobView job = dashboardRow(
                UUID.randomUUID(),
                "com.example.TestJob",
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                0,
                3,
                0,
                null,
                null,
                null,
                null,
                null,
                null);

        Page<JobRepository.DashboardJobView> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findDashboardJobViews(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "FAILED\" onclick=\"alert(1)"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.not(containsString("onclick=\"alert(1)"))));
    }

    @Test
    void shouldReturnHtmlForJobDetailsModal() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setStatus("FAILED");
        job.setType("com.example.FailingJob");
        job.setErrorMessage("Null Pointer Exception");

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));

        mockMvc.perform(get("/jobq/htmx/job/" + jobId))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Null Pointer Exception")))
                .andExpect(content().string(containsString("com.example.FailingJob")));
    }

    @Test
    void shouldRetryFailedJobFromEndpoint() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setStatus("FAILED");
        job.setRetryCount(2);
        job.setErrorMessage("Something went wrong");
        job.setLockedBy("node-abc");
        job.setLockedAt(OffsetDateTime.now().minusMinutes(1));
        job.setRunAt(OffsetDateTime.now().plusHours(1));

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/restart"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(containsString("queued for retry")));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/retry"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"));

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        Job restarted = captor.getValue();

        assertEquals("PENDING", restarted.getStatus());
        assertEquals(0, restarted.getRetryCount());
        assertNull(restarted.getErrorMessage());
        assertNull(restarted.getLockedAt());
        assertNull(restarted.getLockedBy());
        assertNotNull(restarted.getRunAt());
    }

    @Test
    void shouldReturnNotFoundMessageWhenRestartTargetDoesNotExist() throws Exception {
        UUID missingId = UUID.randomUUID();
        when(jobRepository.findById(eq(missingId))).thenReturn(Optional.empty());

        mockMvc.perform(post("/jobq/htmx/job/" + missingId + "/restart"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Job not found")));
    }

    @Test
    void shouldRejectRestartForNonFailedJob() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setStatus("COMPLETED");

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/restart"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("cannot be retried")));

        verify(jobRepository, never()).save(any(Job.class));
    }

    private JobRepository.DashboardJobView dashboardRow(
            UUID id,
            String type,
            OffsetDateTime createdAt,
            OffsetDateTime runAt,
            int retryCount,
            int maxRetries,
            int priority,
            String groupId,
            String replaceKey,
            OffsetDateTime processingStartedAt,
            OffsetDateTime finishedAt,
            OffsetDateTime failedAt,
            String errorMessage) {
        return new JobRepository.DashboardJobView() {
            @Override
            public UUID getId() {
                return id;
            }

            @Override
            public String getType() {
                return type;
            }

            @Override
            public OffsetDateTime getCreatedAt() {
                return createdAt;
            }

            @Override
            public OffsetDateTime getRunAt() {
                return runAt;
            }

            @Override
            public int getRetryCount() {
                return retryCount;
            }

            @Override
            public int getMaxRetries() {
                return maxRetries;
            }

            @Override
            public int getPriority() {
                return priority;
            }

            @Override
            public String getGroupId() {
                return groupId;
            }

            @Override
            public String getReplaceKey() {
                return replaceKey;
            }

            @Override
            public OffsetDateTime getProcessingStartedAt() {
                return processingStartedAt;
            }

            @Override
            public OffsetDateTime getFinishedAt() {
                return finishedAt;
            }

            @Override
            public OffsetDateTime getFailedAt() {
                return failedAt;
            }

            @Override
            public String getErrorMessage() {
                return errorMessage;
            }
        };
    }
}
