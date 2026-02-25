package com.jobq.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
        Job pendingJob = new Job();
        pendingJob.setStatus("PENDING");

        when(jobRepository.count()).thenReturn(1L);
        when(jobRepository.findAll()).thenReturn(List.of(pendingJob));

        mockMvc.perform(get("/jobq/htmx/stats"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Total Jobs")))
                .andExpect(content().string(containsString("Pending")));
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
}
