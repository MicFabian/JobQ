package com.jobq.dashboard;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.TestApplication;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
        jobRepository.deleteAllInBatch();
    }

    @Test
    void shouldRequireAuthenticationForDashboardEndpoints() throws Exception {
        mockMvc.perform(get("/jobq/htmx/jobs")).andExpect(status().isUnauthorized());

        mockMvc.perform(get("/jobq/dashboard")).andExpect(status().isUnauthorized());
    }

    @Test
    void shouldServeFavicon() throws Exception {
        mockMvc.perform(get("/favicon.ico")).andExpect(status().isOk());
    }

    @Test
    void shouldEnforceTimestampDefaultsAndNotNullConstraints() {
        assertTimestampColumnConfigured("created_at");
        assertTimestampColumnConfigured("updated_at");
        assertTimestampColumnConfigured("run_at");
    }

    @Test
    void shouldRenderDashboardIndexWithConvenienceControls() throws Exception {
        mockMvc.perform(get("/jobq/dashboard").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/jobq/assets/index.html"));

        mockMvc.perform(get("/jobq/assets/index.html").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Job Execution Log")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("href=\"#jobq-monitoring\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("href=\"#jobq-management\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("id=\"jobq-monitoring\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("id=\"jobq-management\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("JobQ Management")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"query\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"statusFilter\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"scheduledOnly\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"retriedOnly\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("run-at-asc")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"selectedIds\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"failedSince\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Rerun selected")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Rerun failed since")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("jobq-action-feedback")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("jobq-select-all")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("across pages")));
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

        assertEquals(List.of(4, 1, 1, 1, 1, 0), extractStatsNumbers(html));
    }

    @Test
    void shouldFilterAndSortScheduledPendingJobs() throws Exception {
        Job earliest = persistPending(
                "generate-report-early", OffsetDateTime.now().plusMinutes(5), 0, "reports", "customer-1");
        Job later = persistPending(
                "generate-report-late", OffsetDateTime.now().plusMinutes(10), 1, "reports", "customer-2");
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
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Run now")))
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
    void shouldFilterByStatusCaseInsensitively() throws Exception {
        persistFailed("status-failed-job", "boom", 1);
        persistCompleted("status-completed-job");

        String html = mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("status", "Failed")
                        .param("size", "20")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertTrue(html.contains("status-failed-job"));
        assertFalse(html.contains("status-completed-job"));
    }

    @Test
    void shouldSearchByFailureMessage() throws Exception {
        persistFailed("invoice-email-job", "SMTP timeout while sending welcome email", 2);
        persistFailed("invoice-pdf-job", "PDF generation failed", 1);

        String html = mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("query", "welcome email")
                        .param("size", "20")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertTrue(html.contains("invoice-email-job"));
        assertFalse(html.contains("invoice-pdf-job"));
    }

    @Test
    void shouldSearchByType() throws Exception {
        persistCompleted("send-welcome-email");
        persistCompleted("generate-report");

        String html = mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("query", "send-welcome")
                        .param("size", "20")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertTrue(html.contains("send-welcome-email"));
        assertFalse(html.contains("generate-report"));
    }

    @Test
    void shouldSearchByStatusKeyword() throws Exception {
        persistFailed("keyword-failed-job", "boom", 1);
        persistCompleted("keyword-completed-job");

        String html = mockMvc.perform(get("/jobq/htmx/jobs")
                        .header("Authorization", basicAuthHeader())
                        .param("query", "failed")
                        .param("size", "20")
                        .param("page", "0"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertTrue(html.contains("keyword-failed-job"));
        assertFalse(html.contains("keyword-completed-job"));
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

        mockMvc.perform(post("/jobq/htmx/job/" + failed.getId() + "/retry").header("Authorization", basicAuthHeader()))
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
    void shouldRunDelayedPendingJobImmediately() throws Exception {
        Job delayed = persistPending("delayed-email", OffsetDateTime.now().plusMinutes(15), 0, "mail", "customer-1");

        mockMvc.perform(post("/jobq/htmx/job/" + delayed.getId() + "/run-now")
                        .header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("queued to run immediately")));

        Job reloaded = jobRepository.findById(delayed.getId()).orElseThrow();
        assertNotNull(reloaded.getRunAt());
        assertFalse(reloaded.getRunAt().isAfter(OffsetDateTime.now().plusSeconds(1)));
        assertNotNull(reloaded.getUpdatedAt());
    }

    @Test
    void shouldRejectRunNowForNonDelayedPendingJob() throws Exception {
        Job readyNow = persistPending("already-ready", OffsetDateTime.now().minusMinutes(1), 0, "mail", "customer-2");

        mockMvc.perform(post("/jobq/htmx/job/" + readyNow.getId() + "/run-now")
                        .header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(content()
                        .string(org.hamcrest.Matchers.containsString(
                                "is not delayed in PENDING state and cannot be forced to run now")));

        Job reloaded = jobRepository.findById(readyNow.getId()).orElseThrow();
        assertTrue(reloaded.getRunAt().isBefore(OffsetDateTime.now()));
    }

    @Test
    void shouldRejectRerunForNonTerminalJob() throws Exception {
        Job pending = persistPending("non-failed", OffsetDateTime.now(), 0, null, null);

        mockMvc.perform(post("/jobq/htmx/job/" + pending.getId() + "/retry").header("Authorization", basicAuthHeader()))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("cannot be rerun")));

        Job reloaded = jobRepository.findById(pending.getId()).orElseThrow();
        assertEquals("PENDING", reloaded.getStatus());
    }

    @Test
    void shouldBatchRerunSelectedTerminalJobs() throws Exception {
        Job failed = persistFailed("failed-batch-rerun", "boom", 2);
        Job completed = persistCompleted("completed-batch-rerun");
        Job pending = persistPending("pending-batch-rerun", OffsetDateTime.now().plusMinutes(1), 0, null, null);

        mockMvc.perform(post("/jobq/htmx/jobs/rerun-selected")
                        .header("Authorization", basicAuthHeader())
                        .param("selectedIds", failed.getId() + "," + completed.getId() + "," + pending.getId()))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", org.hamcrest.Matchers.containsString("jobqSelectionCleared")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Queued 2 selected jobs")));

        Job failedReloaded = jobRepository.findById(failed.getId()).orElseThrow();
        assertEquals("PENDING", failedReloaded.getStatus());
        assertEquals(0, failedReloaded.getRetryCount());
        assertNull(failedReloaded.getErrorMessage());

        Job completedReloaded = jobRepository.findById(completed.getId()).orElseThrow();
        assertEquals("PENDING", completedReloaded.getStatus());
        assertEquals(0, completedReloaded.getRetryCount());
        assertNull(completedReloaded.getErrorMessage());

        Job pendingReloaded = jobRepository.findById(pending.getId()).orElseThrow();
        assertEquals("PENDING", pendingReloaded.getStatus());
    }

    @Test
    void shouldBatchRerunFailedJobsByFilterSinceTimestamp() throws Exception {
        Job oldFailed = persistFailed("invoice-old-failed", "legacy timeout", 1);
        oldFailed.setFailedAt(OffsetDateTime.now().minusHours(2));
        oldFailed.setUpdatedAt(OffsetDateTime.now().minusHours(2));
        jobRepository.saveAndFlush(oldFailed);

        Job recentFailedMatching = persistFailed("invoice-recent-failed", "invoice API timeout", 3);
        Job recentFailedNonMatching = persistFailed("shipping-recent-failed", "carrier timeout", 1);

        OffsetDateTime since = OffsetDateTime.now().minusMinutes(30);
        String sinceParam = since.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        mockMvc.perform(post("/jobq/htmx/jobs/rerun-failed-since")
                        .header("Authorization", basicAuthHeader())
                        .param("failedSince", sinceParam)
                        .param("query", "invoice")
                        .param("retriedOnly", "true"))
                .andExpect(status().isOk())
                .andExpect(header().string("HX-Trigger", "jobq-refresh"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Queued 1 failed job since")));

        Job oldFailedReloaded = jobRepository.findById(oldFailed.getId()).orElseThrow();
        assertEquals("FAILED", oldFailedReloaded.getStatus());

        Job recentFailedMatchingReloaded =
                jobRepository.findById(recentFailedMatching.getId()).orElseThrow();
        assertEquals("PENDING", recentFailedMatchingReloaded.getStatus());
        assertEquals(0, recentFailedMatchingReloaded.getRetryCount());
        assertNull(recentFailedMatchingReloaded.getErrorMessage());

        Job recentFailedNonMatchingReloaded =
                jobRepository.findById(recentFailedNonMatching.getId()).orElseThrow();
        assertEquals("FAILED", recentFailedNonMatchingReloaded.getStatus());
    }

    private String basicAuthHeader() {
        String creds = "admin:supersecret";
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    }

    private Job persistPending(String type, OffsetDateTime runAt, int retryCount, String groupId, String replaceKey) {
        Job job = new Job(
                UUID.randomUUID(), type, objectMapper.valueToTree(Map.of("type", type)), 5, 0, groupId, replaceKey);
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

    private void assertTimestampColumnConfigured(String columnName) {
        String defaultValue = jdbcTemplate.queryForObject(
                """
                        SELECT column_default
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'jobq_jobs'
                          AND column_name = ?
                        """,
                String.class,
                columnName);
        assertNotNull(defaultValue);
        String normalizedDefault = defaultValue.toLowerCase();
        assertTrue(normalizedDefault.contains("now()") || normalizedDefault.contains("current_timestamp"));

        String isNullable = jdbcTemplate.queryForObject(
                """
                        SELECT is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'jobq_jobs'
                          AND column_name = ?
                        """,
                String.class,
                columnName);
        assertEquals("NO", isNullable);
    }
}
