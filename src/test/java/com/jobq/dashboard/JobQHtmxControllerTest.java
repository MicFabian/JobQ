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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        when(jobRepository.countPendingJobs()).thenReturn(1L);
        when(jobRepository.countProcessingJobs()).thenReturn(0L);
        when(jobRepository.countCompletedJobs()).thenReturn(0L);
        when(jobRepository.countFailedJobs()).thenReturn(0L);

        mockMvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Total Jobs")))
                .andExpect(content().string(containsString("Pending")));

        verify(jobRepository, never()).findAll();
    }

    @Test
    void shouldReturnHtmlForJobsTable() throws Exception {
        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setStatus("PENDING");
        job.setType("com.example.TestJob");
        job.setRetryCount(1);
        job.setMaxRetries(3);

        Page<Job> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findAll(any(Pageable.class))).thenReturn(jobPage);

        mockMvc.perform(get("/jobq/htmx/jobs"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("com.example.TestJob")))
                .andExpect(content().string(containsString("1 / 3")));
    }

    @Test
    void shouldIgnoreUnknownStatusFilterValue() throws Exception {
        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setType("com.example.TestJob");

        Page<Job> jobPage = new PageImpl<>(List.of(job));
        when(jobRepository.findAll(any(Pageable.class))).thenReturn(jobPage);

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
    void shouldRestartFailedJobFromEndpoint() throws Exception {
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
                .andExpect(content().string(containsString("has been restarted")));

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
                .andExpect(content().string(containsString("cannot be restarted")));

        verify(jobRepository, never()).save(any(Job.class));
    }
}
