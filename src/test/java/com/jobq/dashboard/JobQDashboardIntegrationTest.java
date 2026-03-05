package com.jobq.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.TestApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        classes = TestApplication.class,
        properties = {
                "jobq.background-job-server.enabled=false",
                "jobq.dashboard.enabled=true",
                "jobq.dashboard.username=admin",
                "jobq.dashboard.password=supersecret"
        })
@ActiveProfiles("test")
@Testcontainers
class JobQDashboardIntegrationTest {

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
    WebApplicationContext webApplicationContext;

    MockMvc mockMvc;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
        jobRepository.deleteAllInBatch();
    }

    @Test
    void shouldRequireAuthenticationForDashboardEndpoints() throws Exception {
        mockMvc.perform(get("/jobq/htmx/jobs"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get("/jobq/dashboard"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void shouldRenderDashboardIndexWithConvenienceControls() throws Exception {
        mockMvc.perform(get("/jobq/dashboard").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/jobq/assets/index.html"));

        mockMvc.perform(get("/jobq/assets/index.html").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Job Execution Log")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"query\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"statusFilter\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"scheduledOnly\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"retriedOnly\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("run-at-asc")));
    }

    @Test
    void shouldReturnAccurateLifecycleStats() throws Exception {
        persistPending("pending-job", OffsetDateTime.now().plusMinutes(2), 0, null, null);
        persistProcessing("processing-job");
        persistCompleted("completed-job");
        persistFailed("failed-job", "boom", 2);

        String html = mockMvc.perform(get("/jobq/htmx/stats").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertEquals(List.of(4, 1, 1, 1, 1), extractStatsNumbers(html));
    }

    @Test
    void shouldFilterAndSortScheduledPendingJobs() throws Exception {
        Job earliest = persistPending("generate-report-early", OffsetDateTime.now().plusMinutes(5), 0, "reports",
                "customer-1");
        Job later = persistPending("generate-report-late", OffsetDateTime.now().plusMinutes(10), 1, "reports",
                "customer-2");
        persistPending("other-job", OffsetDateTime.now().minusMinutes(1), 0, "misc", "other");

        String html = mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("status", "PENDING")
                        .param("query", "reports")
                        .param("scheduledOnly", "true")
                        .param("sort", "run-at-asc")
                        .param("size", "20")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", org.hamcrest.Matchers.containsString("\"hasNext\":false")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Runs ")))
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertTrue(html.contains("generate-report-early"));
        assertTrue(html.contains("generate-report-late"));
        assertFalse(html.contains("other-job"));

        int earlyIndex = html.indexOf(earliest.getId().toString().substring(0, 8));
        int laterIndex = html.indexOf(later.getId().toString().substring(0, 8));
        assertTrue(earlyIndex >= 0 && laterIndex >= 0 && earlyIndex < laterIndex);
    }

    @Test
    void shouldReportPaginationHasNextWhenMoreRowsExist() throws Exception {
        persistPending("p1", OffsetDateTime.now().plusMinutes(1), 0, null, null);
        persistPending("p2", OffsetDateTime.now().plusMinutes(2), 0, null, null);
        persistPending("p3", OffsetDateTime.now().plusMinutes(3), 0, null, null);

        mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("size", "2")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", org.hamcrest.Matchers.containsString("\"page\":0")))
                .andExpect(header().string("HX-Trigger", org.hamcrest.Matchers.containsString("\"hasNext\":true")));
    }

    @Test
    void shouldRetryFailedJobAndResetFailureFields() throws Exception {
        Job failed = persistFailed("invoice-export", "Temporary DB error", 3);
        failed.setLockedAt(OffsetDateTime.now().minusMinutes(1));
        failed.setLockedBy("node-a");
        failed.setRunAt(OffsetDateTime.now().plusHours(1));
        jobRepository.saveAndFlush(failed);

        mockMvc.perform(post("/jobq/htmx/job/" + failed.getId() + "/retry")
                        .header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("queued for retry")));

        Job reloaded = jobRepository.findById(failed.getId()).orElseThrow();
        assertNull(reloaded.getProcessingStartedAt());
        assertNull(reloaded.getFinishedAt());
        assertNull(reloaded.getFailedAt());
        assertEquals(0, reloaded.getRetryCount());
        assertNull(reloaded.getErrorMessage());
        assertNull(reloaded.getLockedAt());
        assertNull(reloaded.getLockedBy());
        assertNotNull(reloaded.getRunAt());
        assertNotNull(reloaded.getUpdatedAt());
    }

    @Test
    void shouldRerunCompletedJobAndResetCompletionFields() throws Exception {
        Job completed = persistCompleted("daily-summary");
        completed.setRunAt(OffsetDateTime.now().plusHours(2));
        jobRepository.saveAndFlush(completed);

        mockMvc.perform(post("/jobq/htmx/job/" + completed.getId() + "/rerun")
                        .header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("queued for rerun")));

        Job reloaded = jobRepository.findById(completed.getId()).orElseThrow();
        assertNull(reloaded.getProcessingStartedAt());
        assertNull(reloaded.getFinishedAt());
        assertNull(reloaded.getFailedAt());
        assertEquals(0, reloaded.getRetryCount());
        assertNull(reloaded.getErrorMessage());
        assertNotNull(reloaded.getRunAt());
        assertNotNull(reloaded.getUpdatedAt());
    }

    @Test
    void shouldRejectRerunForNonTerminalJob() throws Exception {
        Job pending = persistPending("non-failed", OffsetDateTime.now(), 0, null, null);

        mockMvc.perform(post("/jobq/htmx/job/" + pending.getId() + "/retry")
                        .header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("cannot be rerun")));

        Job reloaded = jobRepository.findById(pending.getId()).orElseThrow();
        assertEquals("PENDING", reloaded.getStatus());
    }

    private String basicAuthHeader() {
        String creds = "admin:supersecret";
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    }

    private Job persistPending(String type, OffsetDateTime runAt, int retryCount, String groupId, String replaceKey) {
        Job job = new Job(UUID.randomUUID(), type, objectMapper.valueToTree(Map.of("type", type)), 5, 0, groupId,
                replaceKey);
        job.setRunAt(runAt);
        job.setRetryCount(retryCount);
        job.setUpdatedAt(OffsetDateTime.now());
        return jobRepository.saveAndFlush(job);
    }

    private Job persistProcessing(String type) {
        Job job = new Job(UUID.randomUUID(), type, objectMapper.valueToTree(Map.of("type", type)), 5, 0, null, null);
        job.setProcessingStartedAt(OffsetDateTime.now().minusMinutes(2));
        job.setRunAt(OffsetDateTime.now().minusMinutes(3));
        job.setUpdatedAt(OffsetDateTime.now());
        return jobRepository.saveAndFlush(job);
    }

    private Job persistCompleted(String type) {
        Job job = new Job(UUID.randomUUID(), type, objectMapper.valueToTree(Map.of("type", type)), 5, 0, null, null);
        job.setProcessingStartedAt(OffsetDateTime.now().minusMinutes(4));
        job.setFinishedAt(OffsetDateTime.now().minusMinutes(1));
        job.setRunAt(OffsetDateTime.now().minusMinutes(5));
        job.setUpdatedAt(OffsetDateTime.now());
        return jobRepository.saveAndFlush(job);
    }

    private Job persistFailed(String type, String errorMessage, int retryCount) {
        Job job = new Job(UUID.randomUUID(), type, objectMapper.valueToTree(Map.of("type", type)), 5, 0, null, null);
        job.setProcessingStartedAt(OffsetDateTime.now().minusMinutes(4));
        job.setFailedAt(OffsetDateTime.now().minusMinutes(1));
        job.setErrorMessage(errorMessage);
        job.setRetryCount(retryCount);
        job.setRunAt(OffsetDateTime.now().minusMinutes(5));
        job.setUpdatedAt(OffsetDateTime.now());
        return jobRepository.saveAndFlush(job);
    }

    private List<Integer> extractStatsNumbers(String html) {
        Pattern pattern = Pattern.compile("text-3xl font-bold text-slate-100\\\">(\\d+)</div>");
        Matcher matcher = pattern.matcher(html);
        List<Integer> values = new ArrayList<>();
        while (matcher.find()) {
            values.add(Integer.parseInt(matcher.group(1)));
        }
        return values;
    }
}
