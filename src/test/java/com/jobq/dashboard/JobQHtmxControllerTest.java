package com.jobq.dashboard;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.config.JobQProperties;
import com.jobq.internal.JobOperationsService;
import com.jobq.internal.JobSignalPublisher;
import com.jobq.internal.JobTypeMetadataRegistry;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

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

            @Override
            public Long getCancelledCount() {
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
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
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
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "FAILED"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Retry")))
                .andExpect(content().string(containsString("hx-target=\"#jobq-action-feedback\"")))
                .andExpect(content().string(containsString("Failed at")))
                .andExpect(content().string(containsString("Database timeout while writing status row")));
    }

    @Test
    void shouldRenderRerunButtonForCompletedJobs() throws Exception {
        JobRepository.DashboardJobView job = dashboardRow(
                UUID.randomUUID(),
                "com.example.CompletedJob",
                OffsetDateTime.now().minusMinutes(15),
                OffsetDateTime.now().minusMinutes(15),
                0,
                3,
                0,
                null,
                null,
                OffsetDateTime.now().minusMinutes(10),
                OffsetDateTime.now().minusMinutes(1),
                null,
                null);

        Page<JobRepository.DashboardJobView> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "COMPLETED"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Rerun")));
    }

    @Test
    void shouldRenderRunNowButtonForDelayedPendingJobs() throws Exception {
        JobRepository.DashboardJobView job = dashboardRow(
                UUID.randomUUID(),
                "com.example.DelayedJob",
                OffsetDateTime.now().minusMinutes(2),
                OffsetDateTime.now().plusMinutes(10),
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
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "PENDING"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Run now")))
                .andExpect(content().string(containsString("/run-now")));
    }

    @Test
    void shouldRenderWorkerNodesPanelWhenHeartbeatExists() throws Exception {
        JobOperationsService operationsService = mock(JobOperationsService.class);
        JobTypeMetadataRegistry metadataRegistry = mock(JobTypeMetadataRegistry.class);
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        ObjectProvider<JobSignalPublisher> signalPublisherProvider =
                beanFactory.getBeanProvider(JobSignalPublisher.class);
        ObjectProvider<JobDashboardEventBus> dashboardEventBusProvider =
                beanFactory.getBeanProvider(JobDashboardEventBus.class);
        JobQProperties properties = new JobQProperties();
        OffsetDateTime now = OffsetDateTime.now();
        when(operationsService.loadWorkerNodes(any(Duration.class)))
                .thenReturn(List.of(new JobOperationsService.WorkerNodeStatus(
                        "node-123",
                        now.minusMinutes(5),
                        now.minusSeconds(2),
                        8,
                        2,
                        List.of("TYPE_A", "TYPE_B"),
                        true)));

        JobQDashboardController controller = new JobQDashboardController(
                jobRepository,
                objectMapper,
                signalPublisherProvider,
                operationsService,
                metadataRegistry,
                properties,
                new JobPayloadRedactor(properties),
                dashboardEventBusProvider);
        MockMvc localMockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        localMockMvc
                .perform(get("/jobq/htmx/nodes-panel"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Worker Nodes")))
                .andExpect(content().string(containsString("node-123")))
                .andExpect(content().string(containsString("2 / 8")))
                .andExpect(content().string(containsString("TYPE_A, TYPE_B")));
    }

    @Test
    void shouldReturnHtmlForPaginationControls() throws Exception {
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/pagination"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Page 1")))
                .andExpect(content().string(containsString("jobq-refresh")));
    }

    @Test
    void shouldClampInvalidPageAndSizeValues() throws Exception {
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/jobs").param("page", "-7").param("size", "100000"))
                .andExpect(status().isOk());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository)
                .findDashboardJobViews(
                        anyString(),
                        anyString(),
                        anyBoolean(),
                        anyBoolean(),
                        anyBoolean(),
                        any(),
                        pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getPageNumber());
        assertEquals(200, pageable.getPageSize());
        assertTrue(pageable.getSort().isSorted());
    }

    @Test
    void shouldApplySortingAndFilterFlagsToDashboardQuery() throws Exception {
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/jobs")
                        .param("status", "PROCESSING")
                        .param("query", "sync-customer")
                        .param("sort", "priority-desc")
                        .param("scheduledOnly", "true")
                        .param("retriedOnly", "true"))
                .andExpect(status().isOk());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository)
                .findDashboardJobViews(
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
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(delayedJob)));

        mockMvc.perform(get("/jobq/htmx/jobs").param("scheduledOnly", "true").param("sort", "run-at-asc"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Runs ")));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository)
                .findDashboardJobViews(
                        eq(""), eq(""), eq(true), eq(false), eq(false), eq(new UUID(0L, 0L)), pageableCaptor.capture());

        Pageable pageable = pageableCaptor.getValue();
        assertNotNull(pageable.getSort().getOrderFor("runAt"));
        assertTrue(pageable.getSort().getOrderFor("runAt").isAscending());
    }

    @Test
    void shouldTreatUuidSearchAsExactIdMatch() throws Exception {
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());
        UUID searchedId = UUID.randomUUID();

        mockMvc.perform(get("/jobq/htmx/jobs").param("query", searchedId.toString()))
                .andExpect(status().isOk());

        verify(jobRepository)
                .findDashboardJobViews(
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
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "FAILED\" onclick=\"alert(1)"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.not(containsString("onclick=\"alert(1)"))));
    }

    @Test
    void shouldNormalizeMixedCaseStatusFilterBeforeQuery() throws Exception {
        when(jobRepository.findDashboardJobViews(
                        anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any(Pageable.class)))
                .thenReturn(Page.empty());

        mockMvc.perform(get("/jobq/htmx/jobs").param("status", "Failed")).andExpect(status().isOk());

        verify(jobRepository)
                .findDashboardJobViews(
                        eq("FAILED"),
                        anyString(),
                        eq(false),
                        eq(false),
                        eq(false),
                        eq(new UUID(0L, 0L)),
                        any(Pageable.class));
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
    void shouldRenderRunNowButtonInDetailsModalForDelayedPendingJob() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setType("com.example.DelayedJob");
        job.setRunAt(OffsetDateTime.now().plusMinutes(10));

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));

        mockMvc.perform(get("/jobq/htmx/job/" + jobId))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Run Now")))
                .andExpect(content().string(containsString("/job/" + jobId + "/run-now")));
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
    void shouldRerunCompletedJobFromEndpoint() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setStatus("COMPLETED");
        job.setRetryCount(1);
        job.setErrorMessage("old");
        job.setRunAt(OffsetDateTime.now().plusHours(1));

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/rerun"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(containsString("queued for rerun")));

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        Job rerun = captor.getValue();

        assertEquals("PENDING", rerun.getStatus());
        assertEquals(0, rerun.getRetryCount());
        assertNull(rerun.getErrorMessage());
        assertNotNull(rerun.getRunAt());
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
    void shouldRunDelayedPendingJobImmediatelyFromEndpoint() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setRunAt(OffsetDateTime.now().plusMinutes(5));

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/run-now"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(containsString("queued to run immediately")));

        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(captor.capture());
        Job updated = captor.getValue();
        assertNotNull(updated.getRunAt());
        assertFalse(updated.getRunAt().isAfter(OffsetDateTime.now().plusSeconds(1)));
        assertNotNull(updated.getUpdatedAt());
    }

    @Test
    void shouldRejectRunNowWhenJobIsNotDelayedPending() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setRunAt(OffsetDateTime.now().minusMinutes(1));

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/run-now"))
                .andExpect(status().isOk())
                .andExpect(content()
                        .string(containsString("is not delayed in PENDING state and cannot be forced to run now")));

        verify(jobRepository, never()).save(any(Job.class));
    }

    @Test
    void shouldRejectRerunForActiveJob() throws Exception {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setStatus("PROCESSING");

        when(jobRepository.findById(eq(jobId))).thenReturn(Optional.of(job));

        mockMvc.perform(post("/jobq/htmx/job/" + jobId + "/restart"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("cannot be rerun")));

        verify(jobRepository, never()).save(any(Job.class));
    }

    @Test
    void shouldBatchRerunSelectedTerminalJobs() throws Exception {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        when(jobRepository.rerunTerminalJobsByIds(any(), any())).thenReturn(2);

        mockMvc.perform(post("/jobq/htmx/jobs/rerun-selected")
                        .param("selectedIds", first + "," + second + ",not-a-uuid"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", containsString("jobqSelectionCleared")))
                .andExpect(content().string(containsString("Queued 2 selected jobs")));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<UUID>> idsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jobRepository).rerunTerminalJobsByIds(idsCaptor.capture(), any(OffsetDateTime.class));
        assertEquals(List.of(first, second), idsCaptor.getValue());
    }

    @Test
    void shouldRejectBatchRerunWhenSelectionIsEmpty() throws Exception {
        mockMvc.perform(post("/jobq/htmx/jobs/rerun-selected"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Select at least one COMPLETED or FAILED job")));

        verify(jobRepository, never()).rerunTerminalJobsByIds(any(), any(OffsetDateTime.class));
    }

    @Test
    void shouldRerunFailedJobsByFilterSince() throws Exception {
        when(jobRepository.rerunFailedJobsByFilterSince(
                        anyString(), anyBoolean(), anyBoolean(), any(UUID.class), any(OffsetDateTime.class), any()))
                .thenReturn(3);

        mockMvc.perform(post("/jobq/htmx/jobs/rerun-failed-since")
                        .param("failedSince", "2026-03-26T12:00:00")
                        .param("query", "invoice")
                        .param("retriedOnly", "true"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(containsString("Queued 3 failed jobs since")));

        verify(jobRepository)
                .rerunFailedJobsByFilterSince(
                        eq("invoice"),
                        eq(true),
                        eq(false),
                        eq(new UUID(0L, 0L)),
                        any(OffsetDateTime.class),
                        any(OffsetDateTime.class));
    }

    @Test
    void shouldRejectBatchFailedRerunWhenTimestampIsInvalid() throws Exception {
        mockMvc.perform(post("/jobq/htmx/jobs/rerun-failed-since").param("failedSince", "not-a-date"))
                .andExpect(status().isOk())
                .andExpect(
                        content().string(containsString("Provide a valid timestamp for the failed-since batch rerun")));

        verify(jobRepository, never())
                .rerunFailedJobsByFilterSince(
                        anyString(), anyBoolean(), anyBoolean(), any(UUID.class), any(OffsetDateTime.class), any());
    }

    @Test
    void shouldOnlyShowOperationallyRelevantQueuesByDefault() throws Exception {
        JobOperationsService operationsService = mock(JobOperationsService.class);
        JobTypeMetadataRegistry metadataRegistry = mock(JobTypeMetadataRegistry.class);
        JobQProperties properties = new JobQProperties();

        JobQDashboardController controller = new JobQDashboardController(
                jobRepository,
                objectMapper,
                nullProvider(),
                operationsService,
                metadataRegistry,
                properties,
                new JobPayloadRedactor(properties),
                nullProvider());
        MockMvc controllerMockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        when(metadataRegistry.registeredJobTypes()).thenReturn(Set.of("active", "completed-only", "controlled"));
        when(metadataRegistry.recurringMetadata()).thenReturn(Map.of());
        when(operationsService.loadQueueStats(any(), any()))
                .thenReturn(List.of(
                        new JobOperationsService.QueueStats(
                                "active", 1, 0, 0, 0, 0, null, null, false, null, null, null, false),
                        new JobOperationsService.QueueStats(
                                "completed-only", 0, 0, 4, 0, 1, null, null, false, null, null, null, false),
                        new JobOperationsService.QueueStats(
                                "controlled", 0, 0, 0, 0, 0, null, null, true, 2, null, null, false)));

        controllerMockMvc
                .perform(get("/jobq/htmx/queues-panel"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("active")))
                .andExpect(content().string(containsString("controlled")))
                .andExpect(content()
                        .string(containsString("Queues with active work, failures, controls, or cron schedules")))
                .andExpect(content().string(not(containsString("completed-only"))));

        controllerMockMvc
                .perform(get("/jobq/htmx/queues-panel").param("queueScope", "all"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("All registered job types")))
                .andExpect(content().string(containsString("completed-only")));
    }

    @Test
    void shouldRenderMetricsPanel() throws Exception {
        JobOperationsService operationsService = mock(JobOperationsService.class);
        JobQProperties properties = new JobQProperties();

        JobQDashboardController controller = new JobQDashboardController(
                jobRepository,
                objectMapper,
                nullProvider(),
                operationsService,
                mock(JobTypeMetadataRegistry.class),
                properties,
                new JobPayloadRedactor(properties),
                nullProvider());
        MockMvc controllerMockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        when(operationsService.loadDashboardMetrics(any(Duration.class)))
                .thenReturn(new JobOperationsService.DashboardMetricsSnapshot(
                        Duration.ofHours(6),
                        42.0,
                        84.0,
                        126.0,
                        210.0,
                        420.0,
                        840.0,
                        12.5,
                        8.3,
                        20,
                        2,
                        1,
                        List.of(
                                new JobOperationsService.MetricsPoint(
                                        OffsetDateTime.now().minusMinutes(30), 3, 1, 0),
                                new JobOperationsService.MetricsPoint(OffsetDateTime.now(), 4, 0, 1))));

        controllerMockMvc
                .perform(get("/jobq/htmx/metrics-panel"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Failure rate")))
                .andExpect(content().string(containsString("Queue p50")))
                .andExpect(content().string(containsString("Runtime p50")))
                .andExpect(content().string(containsString("12.50 jobs/min")));
    }

    private <T> ObjectProvider<T> nullProvider() {
        return new ObjectProvider<>() {
            @Override
            public T getObject(Object... args) {
                return null;
            }

            @Override
            public T getIfAvailable() {
                return null;
            }

            @Override
            public T getIfUnique() {
                return null;
            }

            @Override
            public T getObject() {
                return null;
            }
        };
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

            @Override
            public OffsetDateTime getCancelledAt() {
                return null;
            }

            @Override
            public Integer getProgressPercent() {
                return null;
            }

            @Override
            public String getProgressMessage() {
                return null;
            }
        };
    }
}
