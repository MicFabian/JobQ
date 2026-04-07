package com.jobq.dashboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import com.jobq.config.JobQProperties;
import com.jobq.internal.JobOperationsService;
import com.jobq.internal.JobSignalPublisher;
import com.jobq.internal.JobTypeMetadataRegistry;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobq/htmx")
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true")
public class JobQDashboardController {

    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private final JobSignalPublisher jobSignalPublisher;
    private final JobOperationsService jobOperationsService;
    private final JobTypeMetadataRegistry jobTypeMetadataRegistry;
    private final JobQProperties properties;
    private final JobPayloadRedactor jobPayloadRedactor;
    private final JobDashboardEventBus dashboardEventBus;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("MMM dd HH:mm:ss");
    private static final DateTimeFormatter EXACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final UUID ZERO_UUID = new UUID(0L, 0L);
    private static final int MAX_QUERY_LENGTH = 200;
    private static final int MAX_BATCH_IDS = 2_000;

    @Autowired
    public JobQDashboardController(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            ObjectProvider<JobSignalPublisher> jobSignalPublisherProvider,
            JobOperationsService jobOperationsService,
            JobTypeMetadataRegistry jobTypeMetadataRegistry,
            JobQProperties properties,
            JobPayloadRedactor jobPayloadRedactor,
            ObjectProvider<JobDashboardEventBus> dashboardEventBusProvider) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.jobSignalPublisher = jobSignalPublisherProvider.getIfAvailable();
        this.jobOperationsService = jobOperationsService;
        this.jobTypeMetadataRegistry = jobTypeMetadataRegistry;
        this.properties = properties;
        this.jobPayloadRedactor = jobPayloadRedactor;
        this.dashboardEventBus = dashboardEventBusProvider.getIfAvailable();
    }

    JobQDashboardController(JobRepository jobRepository, ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.jobSignalPublisher = null;
        this.jobOperationsService = null;
        this.jobTypeMetadataRegistry = null;
        this.properties = new JobQProperties();
        this.jobPayloadRedactor = new JobPayloadRedactor(this.properties);
        this.dashboardEventBus = null;
    }

    @GetMapping(value = "/stats", produces = MediaType.TEXT_HTML_VALUE)
    public String getStats(
            @RequestParam(name = "filter", required = false, defaultValue = "") String filter,
            @RequestParam(name = "status", required = false, defaultValue = "") String status) {
        String effectiveFilter = filter != null && !filter.isBlank() ? filter : status;
        String normalizedFilter = normalizeStatus(effectiveFilter);
        JobRepository.LifecycleCounts lifecycleCounts = jobRepository.countLifecycleCounts();
        long pending = countOrZero(lifecycleCounts.getPendingCount());
        long processing = countOrZero(lifecycleCounts.getProcessingCount());
        long completed = countOrZero(lifecycleCounts.getCompletedCount());
        long failed = countOrZero(lifecycleCounts.getFailedCount());
        long cancelled = countOrZero(lifecycleCounts.getCancelledCount());
        long total = pending + processing + completed + failed + cancelled;

        return """
                <div class="grid grid-cols-2 xl:grid-cols-6 gap-4 mb-8">
                    %s
                    %s
                    %s
                    %s
                    %s
                    %s
                </div>
                """
                .formatted(
                        statCard("Total Jobs", total, "", normalizedFilter, "ring-indigo-500", "text-slate-500", ""),
                        statCard(
                                "Pending",
                                pending,
                                "PENDING",
                                normalizedFilter,
                                "ring-blue-500",
                                "text-blue-500",
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z'></path></svg>"),
                        statCard(
                                "Processing",
                                processing,
                                "PROCESSING",
                                normalizedFilter,
                                "ring-amber-500",
                                "text-amber-500",
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15'></path></svg>"),
                        statCard(
                                "Completed",
                                completed,
                                "COMPLETED",
                                normalizedFilter,
                                "ring-emerald-500",
                                "text-emerald-500",
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M5 13l4 4L19 7'></path></svg>"),
                        statCard(
                                "Failed",
                                failed,
                                "FAILED",
                                normalizedFilter,
                                "ring-rose-500",
                                "text-rose-500",
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'></path></svg>"),
                        statCard(
                                "Cancelled",
                                cancelled,
                                "CANCELLED",
                                normalizedFilter,
                                "ring-slate-500",
                                "text-slate-400",
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M6 18L18 6M6 6l12 12'></path></svg>"));
    }

    private String statCard(
            String title,
            long value,
            String targetFilter,
            String activeFilter,
            String ringClass,
            String textClass,
            String iconHtml) {
        String activeState = activeFilter.equals(targetFilter) ? "ring-2 " + ringClass : "";
        String escapedFilter = escapeJs(targetFilter);
        return """
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 cursor-pointer hover:-translate-y-1 hover:shadow-lg hover:shadow-indigo-500/10 transition-all duration-200 %s"
                     role="button"
                     tabindex="0"
                     hx-on:click="document.querySelectorAll('input[name=status]').forEach(e=>e.value='%s');document.querySelectorAll('select[name=statusFilter]').forEach(e=>e.value='%s');document.querySelectorAll('input[name=page]').forEach(e=>e.value='0');htmx.trigger(document.body,'jobq-refresh')"
                     hx-on:keydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();document.querySelectorAll('input[name=status]').forEach(e=>e.value='%s');document.querySelectorAll('select[name=statusFilter]').forEach(e=>e.value='%s');document.querySelectorAll('input[name=page]').forEach(e=>e.value='0');htmx.trigger(document.body,'jobq-refresh')}">
                    <div class="%s text-xs font-semibold uppercase tracking-wider mb-2 flex items-center gap-1.5 opacity-80">
                        %s %s
                    </div>
                    <div class="text-3xl font-bold text-slate-100">%d</div>
                </div>
                """
                .formatted(
                        activeState,
                        escapedFilter,
                        escapedFilter,
                        escapedFilter,
                        escapedFilter,
                        textClass,
                        iconHtml,
                        title,
                        value);
    }

    @GetMapping(value = "/jobs", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> getJobs(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "50") int size,
            @RequestParam(name = "status", required = false, defaultValue = "") String status,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "sort", required = false, defaultValue = "created-desc") String sort,
            @RequestParam(name = "scheduledOnly", required = false, defaultValue = "false") boolean scheduledOnly,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly) {
        String normalizedStatus = normalizeStatus(status);
        int normalizedPage = Math.max(0, page);
        int normalizedSize = Math.max(1, Math.min(200, size));
        String normalizedQuery = normalizeQuery(query);
        String normalizedSort = normalizeSort(sort);
        Slice<JobRepository.DashboardJobView> jobsPage = loadDashboardJobsSlice(
                normalizedStatus,
                normalizedPage,
                normalizedSize,
                normalizedQuery,
                normalizedSort,
                scheduledOnly,
                retriedOnly);
        OffsetDateTime now = OffsetDateTime.now();

        StringBuilder rows = new StringBuilder(Math.max(512, normalizedSize * 640));

        if (jobsPage.isEmpty()) {
            rows.append(
                    """
                            <tr>
                              <td colspan="10" class="px-6 py-12 text-center text-slate-400">
                                <div class="flex flex-col items-center justify-center">
                                  <svg class="w-10 h-10 mb-3 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"></path></svg>
                                  <span class="text-lg font-medium">No jobs found %s</span>
                                </div>
                              </td>
                            </tr>
                            """
                            .formatted(!normalizedStatus.isBlank() ? "in " + normalizedStatus : ""));
        } else {
            for (JobRepository.DashboardJobView job : jobsPage.getContent()) {
                String statusLabel = resolveStatus(job);
                String timeline = formatTimeline(job, statusLabel, now);
                String failedInfo = formatFailedInfo(job);
                String rowActionButton = rowActionButtonHtml(statusLabel, job.getId(), job.getRunAt(), now);
                rows.append(
                        """
                                <tr class="hover:bg-slate-800/50 transition-colors border-b border-slate-800/50 group cursor-pointer"
                                    data-job-id="%s"
                                    hx-get="/jobq/htmx/job/%s" hx-target="#modal-container" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator">
                                    <td class="px-3 py-3">
                                        <input type="checkbox"
                                               class="jobq-select-row rounded border-slate-600 bg-slate-900 text-indigo-500"
                                               value="%s"
                                               aria-label="Select job for batch rerun"
                                               title="Select job for batch rerun"
                                               onchange="jobqToggleRowSelection(this)"
                                               onclick="event.stopPropagation()">
                                    </td>
                                    <td class="px-6 py-3">
                                        <span class="px-2.5 py-1 rounded-full text-xs font-medium border %s">
                                            %s
                                        </span>
                                    </td>
                                    <td class="px-6 py-3 font-medium text-slate-200">%s</td>
                                    <td class="px-6 py-3 text-center">
                                        <span class="px-2 py-0.5 rounded text-xs %s">%d / %d</span>
                                        <span class="text-xs text-slate-500 ml-2">Pri: %d</span>
                                    </td>
                                    <td class="px-6 py-3 text-slate-400 text-sm">%s</td>
                                    <td class="px-6 py-3 text-slate-400 text-sm">%s</td>
                                    <td class="px-6 py-3 text-slate-400 text-sm">%s</td>
                                    <td class="px-6 py-3 text-slate-400 text-sm max-w-[22rem]">%s</td>
                                    <td class="px-6 py-3 font-mono text-xs text-slate-500">%s</td>
                                    <td class="px-6 py-3 text-right">
                                       <div class="flex items-center justify-end gap-2">
                                         %s
                                         <span class="text-indigo-400 text-xs font-semibold opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-end gap-1">
                                           Details <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path></svg>
                                         </span>
                                       </div>
                                    </td>
                                </tr>
                                """
                                .formatted(
                                        job.getId().toString(),
                                        job.getId().toString(),
                                        job.getId().toString(),
                                        getBadgeClass(statusLabel),
                                        statusLabel,
                                        escapeHtml(job.getType()),
                                        job.getRetryCount() > 0
                                                ? "bg-rose-500/10 text-rose-400 border border-rose-500/20"
                                                : "bg-slate-800 text-slate-400",
                                        job.getRetryCount(),
                                        job.getMaxRetries(),
                                        job.getPriority(),
                                        job.getGroupId() != null ? escapeHtml(job.getGroupId()) : "—",
                                        job.getReplaceKey() != null ? escapeHtml(job.getReplaceKey()) : "—",
                                        timeline,
                                        failedInfo,
                                        job.getId().toString().substring(0, 8) + "...",
                                        rowActionButton));
            }
        }
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header(
                        "HX-Trigger",
                        "{\"jobqPagination\":{\"page\":" + normalizedPage + ",\"hasNext\":" + jobsPage.hasNext() + "}}")
                .body(rows.toString());
    }

    @GetMapping(value = "/pagination", produces = MediaType.TEXT_HTML_VALUE)
    public String getPagination(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "50") int size,
            @RequestParam(name = "status", required = false, defaultValue = "") String status,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "sort", required = false, defaultValue = "created-desc") String sort,
            @RequestParam(name = "scheduledOnly", required = false, defaultValue = "false") boolean scheduledOnly,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly) {
        String normalizedStatus = normalizeStatus(status);
        int normalizedPage = Math.max(0, page);
        int normalizedSize = Math.max(1, Math.min(200, size));
        String normalizedQuery = normalizeQuery(query);
        String normalizedSort = normalizeSort(sort);
        Slice<JobRepository.DashboardJobView> jobsPage = loadDashboardJobsSlice(
                normalizedStatus,
                normalizedPage,
                normalizedSize,
                normalizedQuery,
                normalizedSort,
                scheduledOnly,
                retriedOnly);
        return renderPaginationControls(normalizedStatus, normalizedPage, jobsPage.hasNext());
    }

    @GetMapping(value = "/settings", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> getSettings() {
        return Map.of("readOnly", properties.getDashboard().isReadOnly());
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public org.springframework.web.servlet.mvc.method.annotation.SseEmitter stream() {
        if (dashboardEventBus == null) {
            return new org.springframework.web.servlet.mvc.method.annotation.SseEmitter(1L);
        }
        return dashboardEventBus.createEmitter();
    }

    @GetMapping(value = "/metrics-panel", produces = MediaType.TEXT_HTML_VALUE)
    public String getMetricsPanel() {
        if (jobOperationsService == null) {
            return "";
        }
        JobOperationsService.DashboardMetricsSnapshot metrics =
                jobOperationsService.loadDashboardMetrics(Duration.ofHours(6));
        StringBuilder points = new StringBuilder();
        long maxValue = metrics.points().stream()
                .mapToLong(point -> point.completedCount() + point.failedCount() + point.cancelledCount())
                .max()
                .orElse(1L);
        for (JobOperationsService.MetricsPoint point : metrics.points()) {
            long total = point.completedCount() + point.failedCount() + point.cancelledCount();
            int height = maxValue == 0 ? 4 : (int) Math.max(4, Math.round(((double) total / (double) maxValue) * 52.0));
            points.append(
                    """
                    <div class="flex flex-col items-center gap-1">
                        <div class="w-2.5 rounded-t bg-indigo-400/90" style="height:%dpx"></div>
                        <span class="text-[10px] text-slate-500">%s</span>
                    </div>
                    """
                            .formatted(
                                    height,
                                    point.bucket() != null
                                            ? point.bucket().format(DateTimeFormatter.ofPattern("HH:mm"))
                                            : "—"));
        }

        return """
                <div class="glass-panel rounded-2xl border border-slate-800/70 p-5">
                    <div class="flex items-center justify-between gap-3 mb-4">
                        <div>
                            <h3 class="text-lg font-semibold text-slate-100">Latency & Throughput</h3>
                            <p class="text-xs uppercase tracking-wider text-slate-500">Last 6 hours</p>
                        </div>
                        <div class="text-xs text-slate-500">Failure rate %s%%</div>
                    </div>
                    <div class="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-7 gap-3 mb-4">
                        %s
                        %s
                        %s
                        %s
                        %s
                        %s
                        %s
                    </div>
                    <div class="rounded-xl border border-slate-800 bg-slate-950/40 p-4">
                        <div class="mb-3 flex items-center justify-between text-xs uppercase tracking-wider text-slate-500">
                            <span>Terminal throughput</span>
                            <span>%s jobs/min</span>
                        </div>
                        <div class="flex items-end gap-2 overflow-x-auto pb-1">%s</div>
                    </div>
                </div>
                """
                .formatted(
                        String.format(Locale.ROOT, "%.1f", metrics.failureRatePercent()),
                        metricCard("Queue p50", String.format(Locale.ROOT, "%.0f ms", metrics.queueP50Ms())),
                        metricCard("Queue p95", String.format(Locale.ROOT, "%.0f ms", metrics.queueP95Ms())),
                        metricCard("Queue p99", String.format(Locale.ROOT, "%.0f ms", metrics.queueP99Ms())),
                        metricCard("Runtime p50", String.format(Locale.ROOT, "%.0f ms", metrics.runtimeP50Ms())),
                        metricCard("Runtime p95", String.format(Locale.ROOT, "%.0f ms", metrics.runtimeP95Ms())),
                        metricCard("Completed", Long.toString(metrics.completedCount())),
                        metricCard("Failed / Cancelled", metrics.failedCount() + " / " + metrics.cancelledCount()),
                        String.format(Locale.ROOT, "%.2f", metrics.throughputPerMinute()),
                        points);
    }

    @GetMapping(value = "/queues-panel", produces = MediaType.TEXT_HTML_VALUE)
    public String getQueuesPanel(@RequestParam(name = "queueScope", defaultValue = "interesting") String queueScope) {
        if (jobOperationsService == null || jobTypeMetadataRegistry == null) {
            return "";
        }
        List<JobOperationsService.QueueStats> queues = jobOperationsService.loadQueueStats(
                jobTypeMetadataRegistry.registeredJobTypes(), jobTypeMetadataRegistry.recurringMetadata());
        boolean showAllQueues = "all".equalsIgnoreCase(queueScope);
        List<JobOperationsService.QueueStats> visibleQueues = queues.stream()
                .filter(queue -> showAllQueues || queue.isInteresting())
                .sorted(queueStatsComparator())
                .toList();
        if (visibleQueues.isEmpty()) {
            visibleQueues = queues.stream().sorted(queueStatsComparator()).toList();
        }
        StringBuilder rows = new StringBuilder();
        if (visibleQueues.isEmpty()) {
            rows.append(emptyPanelState("No queues registered yet."));
        } else {
            for (JobOperationsService.QueueStats queue : visibleQueues) {
                rows.append(
                        """
                        <tr class="border-b border-slate-800/60">
                            <td class="px-4 py-3 font-medium text-slate-100">%s</td>
                            <td class="px-4 py-3 text-slate-300">%d / %d</td>
                            <td class="px-4 py-3 text-slate-400">%d</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3">
                                <div class="flex flex-wrap items-center justify-end gap-2">
                                    <input type="hidden" name="paused" value="%s">
                                    <input name="maxConcurrency" type="number" min="1" value="%s"
                                        class="w-20 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-xs text-slate-100"
                                        placeholder="Conc">
                                    <input name="rateLimitPerMinute" type="number" min="1" value="%s"
                                        class="w-24 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-xs text-slate-100"
                                        placeholder="/ min">
                                    <input name="dispatchCooldownMs" type="number" min="0" value="%s"
                                        class="w-24 rounded-md border border-slate-700 bg-slate-950/60 px-2 py-1 text-xs text-slate-100"
                                        placeholder="Cooldown">
                                    <button class="px-2.5 py-1 rounded-md border border-indigo-500/30 bg-indigo-500/10 text-indigo-300 text-xs font-semibold hover:bg-indigo-500/20 transition-colors"
                                            hx-post="/jobq/htmx/queue/%s/control"
                                            hx-target="#jobq-action-feedback"
                                            hx-swap="innerHTML"
                                            hx-include="closest td"
                                            hx-indicator="#jobq-loading-indicator">Apply</button>
                                    <button class="px-2.5 py-1 rounded-md border %s text-xs font-semibold transition-colors"
                                            hx-post="/jobq/htmx/queue/%s/control"
                                            hx-vals='{"paused":%s}'
                                            hx-target="#jobq-action-feedback"
                                            hx-swap="innerHTML"
                                            hx-include="closest td"
                                            hx-indicator="#jobq-loading-indicator">%s</button>
                                </div>
                            </td>
                        </tr>
                        """
                                .formatted(
                                        escapeHtml(queue.type()),
                                        queue.pendingCount(),
                                        queue.processingCount(),
                                        queue.failedCount(),
                                        queue.paused() ? "Paused" : "Active",
                                        queue.maxConcurrency() != null ? queue.maxConcurrency() : "auto",
                                        queue.rateLimitPerMinute() != null
                                                ? queue.rateLimitPerMinute() + "/min"
                                                : "unlimited",
                                        Boolean.toString(queue.paused()),
                                        queue.maxConcurrency() != null ? queue.maxConcurrency() : "",
                                        queue.rateLimitPerMinute() != null ? queue.rateLimitPerMinute() : "",
                                        queue.dispatchCooldownMs() != null ? queue.dispatchCooldownMs() : "",
                                        escapeHtml(queue.type()),
                                        queue.paused()
                                                ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300 hover:bg-emerald-500/20"
                                                : "border-amber-500/30 bg-amber-500/10 text-amber-300 hover:bg-amber-500/20",
                                        escapeHtml(queue.type()),
                                        Boolean.toString(!queue.paused()),
                                        queue.paused() ? "Resume" : "Pause"));
            }
        }

        return """
                <div class="glass-panel rounded-2xl border border-slate-800/70 overflow-hidden">
                    <div class="px-5 py-4 border-b border-slate-800/60 flex items-center justify-between gap-4">
                        <div>
                            <h3 class="text-lg font-semibold text-slate-100">Queues</h3>
                            <p class="text-xs uppercase tracking-wider text-slate-500">%s</p>
                        </div>
                        <div class="flex items-center gap-2">
                            <span class="text-xs text-slate-500">%d shown</span>
                            <button type="button"
                                    class="px-2.5 py-1 rounded-md border border-slate-700 bg-slate-900/70 text-slate-200 text-xs font-semibold hover:bg-slate-800 transition-colors"
                                    onclick="jobqToggleQueueScope()">
                                %s
                            </button>
                        </div>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="bg-slate-950/40 text-slate-500 uppercase tracking-wider text-xs">
                                <tr>
                                    <th class="px-4 py-3 text-left">Type</th>
                                    <th class="px-4 py-3 text-left">Pending / Processing</th>
                                    <th class="px-4 py-3 text-left">Failed</th>
                                    <th class="px-4 py-3 text-left">State</th>
                                    <th class="px-4 py-3 text-left">Concurrency</th>
                                    <th class="px-4 py-3 text-left">Rate</th>
                                    <th class="px-4 py-3 text-right">Control</th>
                                </tr>
                            </thead>
                            <tbody>%s</tbody>
                        </table>
                    </div>
                </div>
                """
                .formatted(
                        showAllQueues
                                ? "All registered job types"
                                : "Queues with active work, failures, controls, or cron schedules",
                        visibleQueues.size(),
                        showAllQueues ? "Show relevant" : "Show all",
                        rows);
    }

    @GetMapping(value = "/recurring-panel", produces = MediaType.TEXT_HTML_VALUE)
    public String getRecurringPanel() {
        if (jobOperationsService == null || jobTypeMetadataRegistry == null) {
            return "";
        }
        List<JobOperationsService.RecurringSchedule> recurringSchedules =
                jobOperationsService.loadRecurringSchedules(jobTypeMetadataRegistry.recurringMetadata());
        StringBuilder rows = new StringBuilder();
        if (recurringSchedules.isEmpty()) {
            rows.append(emptyPanelState("No recurring jobs configured."));
        } else {
            for (JobOperationsService.RecurringSchedule schedule : recurringSchedules) {
                rows.append(
                        """
                        <tr class="border-b border-slate-800/60">
                            <td class="px-4 py-3 font-medium text-slate-100">%s</td>
                            <td class="px-4 py-3 font-mono text-xs text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%d</td>
                        </tr>
                        """
                                .formatted(
                                        escapeHtml(schedule.type()),
                                        escapeHtml(schedule.cron()),
                                        schedule.nextRunAt() != null
                                                ? schedule.nextRunAt().format(EXACT_FORMATTER)
                                                : "—",
                                        escapeHtml(schedule.misfirePolicy().name()),
                                        schedule.activeCount()));
            }
        }
        return """
                <div class="glass-panel rounded-2xl border border-slate-800/70 overflow-hidden">
                    <div class="px-5 py-4 border-b border-slate-800/60">
                        <h3 class="text-lg font-semibold text-slate-100">Recurring</h3>
                        <p class="text-xs uppercase tracking-wider text-slate-500">Cron schedules and misfire policy</p>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="bg-slate-950/40 text-slate-500 uppercase tracking-wider text-xs">
                                <tr>
                                    <th class="px-4 py-3 text-left">Type</th>
                                    <th class="px-4 py-3 text-left">Cron</th>
                                    <th class="px-4 py-3 text-left">Next Run</th>
                                    <th class="px-4 py-3 text-left">Misfire</th>
                                    <th class="px-4 py-3 text-left">Active</th>
                                </tr>
                            </thead>
                            <tbody>%s</tbody>
                        </table>
                    </div>
                </div>
                """
                .formatted(rows);
    }

    @GetMapping(value = "/nodes-panel", produces = MediaType.TEXT_HTML_VALUE)
    public String getNodesPanel() {
        if (jobOperationsService == null) {
            return "";
        }
        List<JobOperationsService.WorkerNodeStatus> nodes = jobOperationsService.loadWorkerNodes(Duration.ofSeconds(
                Math.max(15, properties.getBackgroundJobServer().getNodeHeartbeatIntervalInSeconds() * 3)));
        StringBuilder rows = new StringBuilder();
        if (nodes.isEmpty()) {
            rows.append(emptyPanelState("No active worker nodes reported recently."));
        } else {
            for (JobOperationsService.WorkerNodeStatus node : nodes) {
                rows.append(
                        """
                        <tr class="border-b border-slate-800/60">
                            <td class="px-4 py-3 font-mono text-xs text-slate-300">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%d / %d</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                        </tr>
                        """
                                .formatted(
                                        escapeHtml(node.nodeId()),
                                        node.lastSeenAt() != null
                                                ? node.lastSeenAt().format(FORMATTER)
                                                : "—",
                                        node.activeProcessingCount(),
                                        node.workerCapacity(),
                                        node.notifyEnabled() ? "LISTEN/NOTIFY" : "Polling only"));
            }
        }
        return """
                <div class="glass-panel rounded-2xl border border-slate-800/70 overflow-hidden">
                    <div class="px-5 py-4 border-b border-slate-800/60">
                        <h3 class="text-lg font-semibold text-slate-100">Worker Nodes</h3>
                        <p class="text-xs uppercase tracking-wider text-slate-500">Cluster heartbeat</p>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="bg-slate-950/40 text-slate-500 uppercase tracking-wider text-xs">
                                <tr>
                                    <th class="px-4 py-3 text-left">Node</th>
                                    <th class="px-4 py-3 text-left">Last Seen</th>
                                    <th class="px-4 py-3 text-left">Active / Capacity</th>
                                    <th class="px-4 py-3 text-left">Dispatch Mode</th>
                                </tr>
                            </thead>
                            <tbody>%s</tbody>
                        </table>
                    </div>
                </div>
                """
                .formatted(rows);
    }

    @GetMapping(value = "/audit-panel", produces = MediaType.TEXT_HTML_VALUE)
    public String getAuditPanel() {
        if (jobOperationsService == null) {
            return "";
        }
        List<JobOperationsService.AuditEntry> entries = jobOperationsService.loadRecentAuditEntries(12);
        StringBuilder rows = new StringBuilder();
        if (entries.isEmpty()) {
            rows.append(emptyPanelState("No dashboard actions recorded yet."));
        } else {
            for (JobOperationsService.AuditEntry entry : entries) {
                rows.append(
                        """
                        <tr class="border-b border-slate-800/60">
                            <td class="px-4 py-3 text-slate-200">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-400">%s</td>
                            <td class="px-4 py-3 text-slate-500">%s</td>
                        </tr>
                        """
                                .formatted(
                                        escapeHtml(entry.action()),
                                        escapeHtml(entry.subject() != null ? entry.subject() : "—"),
                                        escapeHtml(entry.username()),
                                        entry.createdAt() != null
                                                ? entry.createdAt().format(FORMATTER)
                                                : "—"));
            }
        }
        return """
                <div class="glass-panel rounded-2xl border border-slate-800/70 overflow-hidden">
                    <div class="px-5 py-4 border-b border-slate-800/60">
                        <h3 class="text-lg font-semibold text-slate-100">Audit Trail</h3>
                        <p class="text-xs uppercase tracking-wider text-slate-500">Recent dashboard actions</p>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="bg-slate-950/40 text-slate-500 uppercase tracking-wider text-xs">
                                <tr>
                                    <th class="px-4 py-3 text-left">Action</th>
                                    <th class="px-4 py-3 text-left">Subject</th>
                                    <th class="px-4 py-3 text-left">Actor</th>
                                    <th class="px-4 py-3 text-left">At</th>
                                </tr>
                            </thead>
                            <tbody>%s</tbody>
                        </table>
                    </div>
                </div>
                """
                .formatted(rows);
    }

    @GetMapping(value = "/job/{id}/runtime", produces = MediaType.TEXT_HTML_VALUE)
    public String getJobRuntime(@PathVariable("id") UUID id) {
        Job job = jobRepository.findById(id).orElse(null);
        if (job == null) {
            return "<div class=\"text-sm text-slate-500\">Job not found.</div>";
        }
        List<JobOperationsService.JobLogEntry> logs =
                jobOperationsService != null ? jobOperationsService.loadLatestJobLogs(id, 40) : List.of();
        StringBuilder logLines = new StringBuilder();
        if (logs.isEmpty()) {
            logLines.append("<div class=\"text-sm text-slate-500\">No runtime logs captured.</div>");
        } else {
            for (JobOperationsService.JobLogEntry entry : logs) {
                logLines.append(
                        """
                        <div class="flex items-start gap-3 border-b border-slate-800/60 py-2">
                            <span class="mt-0.5 rounded px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider %s">%s</span>
                            <div class="min-w-0 flex-1">
                                <div class="text-xs text-slate-500">%s</div>
                                <div class="text-sm text-slate-200 break-words">%s</div>
                            </div>
                        </div>
                        """
                                .formatted(
                                        logLevelClass(entry.level()),
                                        escapeHtml(entry.level()),
                                        entry.createdAt() != null
                                                ? entry.createdAt().format(EXACT_FORMATTER)
                                                : "—",
                                        escapeHtml(entry.message())));
            }
        }

        int progress = job.getProgressPercent() == null ? 0 : Math.max(0, Math.min(100, job.getProgressPercent()));
        String progressLabel =
                job.getProgressMessage() == null || job.getProgressMessage().isBlank()
                        ? ("PROCESSING".equals(job.getStatus()) ? "Running" : "No progress reported")
                        : escapeHtml(job.getProgressMessage());

        return """
                <div class="space-y-4">
                    <div class="rounded-xl border border-slate-800 bg-slate-950/40 p-4">
                        <div class="mb-2 flex items-center justify-between text-xs uppercase tracking-wider text-slate-500">
                            <span>Runtime</span>
                            <span>%d%%</span>
                        </div>
                        <div class="h-2 rounded-full bg-slate-800 overflow-hidden">
                            <div class="h-full rounded-full bg-gradient-to-r from-indigo-500 via-cyan-400 to-emerald-400 transition-all duration-300" style="width:%d%%"></div>
                        </div>
                        <div class="mt-2 text-sm text-slate-300">%s</div>
                    </div>
                    <div class="rounded-xl border border-slate-800 bg-slate-950/40 p-4">
                        <div class="mb-2 flex items-center justify-between">
                            <h4 class="text-sm font-semibold text-slate-100">Runtime Log</h4>
                            <span class="text-xs uppercase tracking-wider text-slate-500">%d lines</span>
                        </div>
                        <div class="max-h-72 overflow-y-auto">%s</div>
                    </div>
                </div>
                """
                .formatted(progress, progress, progressLabel, logs.size(), logLines);
    }

    @GetMapping(value = "/job/{id}", produces = MediaType.TEXT_HTML_VALUE)
    public String getJobDetails(@PathVariable("id") UUID id) {
        return jobRepository
                .findById(id)
                .map(job -> {
                    String statusLabel = job.getStatus();
                    String payloadHtml = formatPayload(jobPayloadRedactor.redact(job.getPayload()));
                    return """
                    <div class="fixed inset-0 z-50 overflow-hidden" aria-labelledby="slide-over-title" role="dialog" aria-modal="true">
                        <div class="absolute inset-0 bg-slate-950/80 backdrop-blur-sm transition-opacity cursor-pointer flex justify-start" onclick="document.getElementById('modal-container').innerHTML=''">
                            <button class="absolute top-4 left-4 p-2 text-slate-400 hover:text-white bg-slate-800 rounded-full bg-opacity-50">
                                 <svg class="w-6 h-6" fill="none" stroke="currentColor"><path d="M6 18L18 6M6 6l12 12" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"/></svg>
                            </button>
                        </div>
                        <div class="pointer-events-none fixed inset-y-0 right-0 flex max-w-full pl-10 sm:pl-16">
                            <div class="pointer-events-auto w-screen max-w-lg transform transition-transform duration-300 ease-in-out border-l border-slate-800">
                                <div class="flex h-full flex-col overflow-y-scroll bg-slate-900 shadow-2xl">
                                    <div class="bg-slate-950 px-6 py-6 border-b border-indigo-500/30">
                                        <div class="flex items-center justify-between">
                                            <h2 class="text-xl font-bold text-white tracking-tight">Job Details</h2>
                                            <span class="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-widest border %s">%s</span>
                                        </div>
                                        <p class="mt-2 text-sm text-slate-400 font-mono break-all">%s</p>
                                    </div>
                                    <div class="relative flex-1 px-6 py-8">
                                        <div class="space-y-6">
                                            <div class="grid grid-cols-2 gap-4">
                                                %s
                                                %s
                                                %s
                                                %s
                                            </div>
                                            <div class="grid grid-cols-2 gap-4">
                                                %s
                                                %s
                                            </div>
                                            <div class="grid grid-cols-2 gap-4">
                                                %s
                                                %s
                                                %s
                                                %s
                                                %s
                                                %s
                                            </div>
                                            <div id="job-runtime-panel"
                                                 hx-get="/jobq/htmx/job/%s/runtime"
                                                 hx-trigger="load, every 3s [!document.hidden]"
                                                 hx-swap="innerHTML">
                                            </div>
                                            %s
                                            <div>
                                                <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Payload (JSON)</dt>
                                                <dd>
                                                    <pre class="bg-[#0d1117] text-[#c9d1d9] p-4 rounded-lg text-xs font-mono overflow-auto max-h-96 shadow-inner border border-slate-800">%s</pre>
                                                </dd>
                                            </div>
                                            %s
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """
                            .formatted(
                                    getBadgeClass(statusLabel),
                                    statusLabel,
                                    job.getId().toString(),
                                    detailCard("Type", escapeHtml(job.getType())),
                                    detailCard("Priority", Integer.toString(job.getPriority())),
                                    detailCard(
                                            "Retries",
                                            "<span class=\"%s\">%d</span> <span class=\"text-slate-500\">/ %d</span>"
                                                    .formatted(
                                                            job.getRetryCount() > 0
                                                                    ? "text-rose-400"
                                                                    : "text-slate-200",
                                                            job.getRetryCount(),
                                                            job.getMaxRetries())),
                                    detailCard(
                                            "Locked By",
                                            job.getLockedBy() != null ? escapeHtml(job.getLockedBy()) : "—"),
                                    detailCard("Group", job.getGroupId() != null ? escapeHtml(job.getGroupId()) : "—"),
                                    detailCard(
                                            "Replace Key",
                                            job.getReplaceKey() != null ? escapeHtml(job.getReplaceKey()) : "—"),
                                    detailCard("Created At", formatExactTimestamp(job.getCreatedAt())),
                                    detailCard("Run At", formatExactTimestamp(job.getRunAt())),
                                    detailCard("Started At", formatExactTimestamp(job.getProcessingStartedAt())),
                                    detailCard("Finished At", formatExactTimestamp(job.getFinishedAt())),
                                    detailCard("Failed At", formatExactTimestamp(job.getFailedAt())),
                                    detailCard("Cancelled At", formatExactTimestamp(job.getCancelledAt())),
                                    job.getId().toString(),
                                    detailErrorMessage(job.getErrorMessage(), job.getCancelledAt() != null),
                                    payloadHtml,
                                    restartButtonHtml(job));
                })
                .orElse("<div class='p-4 text-red-500'>Job not found.</div>");
    }

    @PostMapping(
            value = {"/job/{id}/restart", "/job/{id}/retry", "/job/{id}/rerun"},
            produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> rerunJob(@PathVariable("id") UUID id, HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Rerun is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        String body = jobRepository
                .findById(id)
                .map(job -> {
                    boolean failed = job.getFailedAt() != null;
                    boolean completed = job.getFinishedAt() != null;
                    boolean cancelled = job.getCancelledAt() != null;
                    if (!failed && !completed && !cancelled) {
                        return """
                        <div class="pointer-events-auto mb-3 rounded-xl border border-amber-500/30 bg-slate-900/95 px-4 py-3 text-center text-amber-300 font-semibold shadow-2xl backdrop-blur-sm">
                            Job %s is not terminal and cannot be rerun.
                        </div>
                        """
                                .formatted(id.toString().substring(0, 8));
                    }

                    job.setProcessingStartedAt(null);
                    job.setFinishedAt(null);
                    job.setFailedAt(null);
                    job.setCancelledAt(null);
                    job.setRetryCount(0);
                    job.setErrorMessage(null);
                    job.setLockedAt(null);
                    job.setLockedBy(null);
                    job.setProgressPercent(null);
                    job.setProgressMessage(null);
                    job.setRunAt(java.time.OffsetDateTime.now());
                    job.setUpdatedAt(java.time.OffsetDateTime.now());
                    jobRepository.save(job);
                    notifyJobType(job.getType());
                    broadcastDashboardRefresh(id);
                    audit(request, "RERUN_JOB", job.getType(), "jobId=" + job.getId());
                    return """
                    <div class="pointer-events-auto mb-3 rounded-xl border border-emerald-500/30 bg-slate-900/95 px-4 py-3 text-center text-emerald-300 font-semibold shadow-2xl backdrop-blur-sm">
                        ✅ Job %s has been queued for %s. It will be picked up on the next poll cycle.
                    </div>
                    """
                            .formatted(
                                    id.toString().substring(0, 8), failed ? "retry" : cancelled ? "replay" : "rerun");
                })
                .orElse("<div class='p-4 text-red-500'>Job not found.</div>");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header("HX-Trigger", "jobq-refresh")
                .body(body);
    }

    @PostMapping(value = "/jobs/rerun-selected", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> rerunSelectedJobs(
            @RequestParam(name = "selectedIds", required = false, defaultValue = "") String selectedIdsRaw,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Batch rerun is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        List<UUID> selectedIds = parseSelectedIds(selectedIdsRaw);
        if (selectedIds.isEmpty()) {
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                    .body(
                            """
                          <div class="pointer-events-auto mb-3 rounded-xl border border-amber-500/30 bg-slate-900/95 px-4 py-3 text-center text-amber-300 font-semibold shadow-2xl backdrop-blur-sm">
                              Select at least one COMPLETED or FAILED job to rerun.
                          </div>
                          """);
        }

        OffsetDateTime now = OffsetDateTime.now();
        int queued = jobRepository.rerunTerminalJobsByIds(selectedIds, now);
        if (queued > 0) {
            notifyAllJobs();
            broadcastDashboardRefresh(null);
            audit(request, "RERUN_SELECTED", "jobs", "count=" + queued);
        }
        int skipped = selectedIds.size() - queued;
        String body =
                """
                <div class="pointer-events-auto mb-3 rounded-xl border border-emerald-500/30 bg-slate-900/95 px-4 py-3 text-center text-emerald-300 font-semibold shadow-2xl backdrop-blur-sm">
                    ✅ Queued %d selected job%s for rerun%s.
                </div>
                """
                        .formatted(
                                queued,
                                queued == 1 ? "" : "s",
                                skipped > 0 ? " (%d skipped: not found or not terminal)".formatted(skipped) : "");

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header("HX-Trigger", "{\"jobq-refresh\":true,\"jobqSelectionCleared\":true}")
                .body(body);
    }

    @PostMapping(value = "/jobs/rerun-failed-since", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> rerunFailedJobsSince(
            @RequestParam(name = "failedSince", required = false, defaultValue = "") String failedSinceRaw,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Failed-job rerun is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        OffsetDateTime failedSince = parseFailedSince(failedSinceRaw);
        if (failedSince == null) {
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                    .body(
                            """
                          <div class="pointer-events-auto mb-3 rounded-xl border border-amber-500/30 bg-slate-900/95 px-4 py-3 text-center text-amber-300 font-semibold shadow-2xl backdrop-blur-sm">
                              Provide a valid timestamp for the failed-since batch rerun.
                          </div>
                          """);
        }

        OffsetDateTime now = OffsetDateTime.now();
        String normalizedQuery = normalizeQuery(query);
        UUID queriedJobId = parseUuid(normalizedQuery);
        boolean jobIdProvided = queriedJobId != null;
        int queued = jobRepository.rerunFailedJobsByFilterSince(
                normalizedQuery,
                retriedOnly,
                jobIdProvided,
                jobIdProvided ? queriedJobId : ZERO_UUID,
                failedSince,
                now);
        if (queued > 0) {
            notifyAllJobs();
            broadcastDashboardRefresh(null);
            audit(
                    request,
                    "RERUN_FAILED_SINCE",
                    "jobs",
                    "count=" + queued + ",failedSince=" + failedSince.format(EXACT_FORMATTER));
        }
        String body =
                """
                <div class="pointer-events-auto mb-3 rounded-xl border border-emerald-500/30 bg-slate-900/95 px-4 py-3 text-center text-emerald-300 font-semibold shadow-2xl backdrop-blur-sm">
                    ✅ Queued %d failed job%s since %s for rerun.
                </div>
                """
                        .formatted(queued, queued == 1 ? "" : "s", failedSince.format(EXACT_FORMATTER));

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header("HX-Trigger", "jobq-refresh")
                .body(body);
    }

    @PostMapping(value = "/job/{id}/run-now", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> runJobNow(@PathVariable("id") UUID id, HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Run-now is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        OffsetDateTime now = OffsetDateTime.now();
        String body = jobRepository
                .findById(id)
                .map(job -> {
                    if (!isDelayedPending(
                            job.getProcessingStartedAt(),
                            job.getFinishedAt(),
                            job.getFailedAt(),
                            job.getCancelledAt(),
                            job.getRunAt(),
                            now)) {
                        return """
                        <div class="pointer-events-auto mb-3 rounded-xl border border-amber-500/30 bg-slate-900/95 px-4 py-3 text-center text-amber-300 font-semibold shadow-2xl backdrop-blur-sm">
                            Job %s is not delayed in PENDING state and cannot be forced to run now.
                        </div>
                        """
                                .formatted(id.toString().substring(0, 8));
                    }
                    job.setRunAt(now);
                    job.setUpdatedAt(now);
                    jobRepository.save(job);
                    notifyJobType(job.getType());
                    broadcastDashboardRefresh(id);
                    audit(request, "RUN_NOW", job.getType(), "jobId=" + job.getId());
                    return """
                    <div class="pointer-events-auto mb-3 rounded-xl border border-emerald-500/30 bg-slate-900/95 px-4 py-3 text-center text-emerald-300 font-semibold shadow-2xl backdrop-blur-sm">
                        ✅ Job %s has been queued to run immediately.
                    </div>
                    """
                            .formatted(id.toString().substring(0, 8));
                })
                .orElse("<div class='p-4 text-red-500'>Job not found.</div>");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header("HX-Trigger", "jobq-refresh")
                .body(body);
    }

    @PostMapping(value = "/job/{id}/cancel", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> cancelJob(@PathVariable("id") UUID id, HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Cancellation is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        Job job = jobRepository.findById(id).orElse(null);
        if (job == null) {
            return htmlResponse("<div class='p-4 text-red-500'>Job not found.</div>", "jobq-refresh");
        }
        int cancelled = jobOperationsService.cancelJob(id, "Cancelled from dashboard", OffsetDateTime.now());
        if (cancelled > 0) {
            broadcastDashboardRefresh(id);
            audit(request, "CANCEL_JOB", job.getType(), "jobId=" + id);
        }
        return htmlResponse(
                """
                <div class="pointer-events-auto mb-3 rounded-xl border border-slate-500/30 bg-slate-900/95 px-4 py-3 text-center text-slate-200 font-semibold shadow-2xl backdrop-blur-sm">
                    %s
                </div>
                """
                        .formatted(
                                cancelled > 0
                                        ? "Cancelled job " + id.toString().substring(0, 8) + "."
                                        : "Job could not be cancelled because it is already terminal."),
                "jobq-refresh");
    }

    @PostMapping(value = "/jobs/cancel-filtered", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> cancelFilteredJobs(
            @RequestParam(name = "status", required = false, defaultValue = "") String status,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "scheduledOnly", required = false, defaultValue = "false") boolean scheduledOnly,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Batch cancellation is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        int cancelled = jobOperationsService.cancelJobsByFilter(
                status,
                normalizeQuery(query),
                scheduledOnly,
                retriedOnly,
                OffsetDateTime.now(),
                "Cancelled from dashboard");
        if (cancelled > 0) {
            broadcastDashboardRefresh(null);
            audit(request, "CANCEL_FILTERED", "jobs", "count=" + cancelled);
        }
        return htmlResponse(
                successMessage(cancelled + " job" + (cancelled == 1 ? "" : "s") + " cancelled."), "jobq-refresh");
    }

    @PostMapping(value = "/jobs/rerun-filtered", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> rerunFilteredJobs(
            @RequestParam(name = "status", required = false, defaultValue = "") String status,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "scheduledOnly", required = false, defaultValue = "false") boolean scheduledOnly,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Filtered rerun is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        int queued = jobOperationsService.rerunTerminalJobsByFilter(
                status, normalizeQuery(query), scheduledOnly, retriedOnly, OffsetDateTime.now());
        if (queued > 0) {
            notifyAllJobs();
            broadcastDashboardRefresh(null);
            audit(request, "RERUN_FILTERED", "jobs", "count=" + queued);
        }
        return htmlResponse(
                successMessage(queued + " filtered terminal job" + (queued == 1 ? "" : "s") + " queued."),
                "jobq-refresh");
    }

    @PostMapping(value = "/jobs/run-filtered-now", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> runFilteredJobsNow(
            @RequestParam(name = "status", required = false, defaultValue = "") String status,
            @RequestParam(name = "query", required = false, defaultValue = "") String query,
            @RequestParam(name = "scheduledOnly", required = false, defaultValue = "false") boolean scheduledOnly,
            @RequestParam(name = "retriedOnly", required = false, defaultValue = "false") boolean retriedOnly,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Run-now is disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        int queued = jobOperationsService.runDelayedJobsByFilterNow(
                status, normalizeQuery(query), scheduledOnly, retriedOnly, OffsetDateTime.now());
        if (queued > 0) {
            notifyAllJobs();
            broadcastDashboardRefresh(null);
            audit(request, "RUN_FILTERED_NOW", "jobs", "count=" + queued);
        }
        return htmlResponse(
                successMessage(queued + " delayed job" + (queued == 1 ? "" : "s") + " released immediately."),
                "jobq-refresh");
    }

    @PostMapping(value = "/queue/{type}/control", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> updateQueueControl(
            @PathVariable("type") String type,
            @RequestParam(name = "paused", required = false, defaultValue = "false") boolean paused,
            @RequestParam(name = "maxConcurrency", required = false) Integer maxConcurrency,
            @RequestParam(name = "rateLimitPerMinute", required = false) Integer rateLimitPerMinute,
            @RequestParam(name = "dispatchCooldownMs", required = false) Integer dispatchCooldownMs,
            HttpServletRequest request) {
        ResponseEntity<String> readOnlyResponse =
                rejectIfReadOnly("Queue control changes are disabled while the dashboard is read-only.");
        if (readOnlyResponse != null) {
            return readOnlyResponse;
        }
        jobOperationsService.upsertQueueControl(
                type, paused, maxConcurrency, rateLimitPerMinute, dispatchCooldownMs, OffsetDateTime.now());
        if (!paused) {
            notifyAllJobs();
        }
        broadcastDashboardRefresh(null);
        audit(
                request,
                "QUEUE_CONTROL",
                type,
                "paused=" + paused + ",maxConcurrency=" + maxConcurrency + ",rateLimitPerMinute=" + rateLimitPerMinute
                        + ",dispatchCooldownMs=" + dispatchCooldownMs);
        return htmlResponse(successMessage("Updated queue control for " + escapeHtml(type) + "."), "jobq-refresh");
    }

    private void notifyJobType(String jobType) {
        if (jobSignalPublisher != null) {
            jobSignalPublisher.notifyJobType(jobType);
        }
    }

    private void notifyAllJobs() {
        if (jobSignalPublisher != null) {
            jobSignalPublisher.notifyAllTypes();
        }
        broadcastDashboardRefresh(null);
    }

    private void broadcastDashboardRefresh(UUID jobId) {
        if (dashboardEventBus != null) {
            if (jobId != null) {
                dashboardEventBus.publishJobRuntime(jobId);
            }
            dashboardEventBus.publishRefresh();
        }
    }

    private ResponseEntity<String> rejectIfReadOnly(String message) {
        if (!properties.getDashboard().isReadOnly()) {
            return null;
        }
        return htmlResponse(
                """
                <div class="pointer-events-auto mb-3 rounded-xl border border-amber-500/30 bg-slate-900/95 px-4 py-3 text-center text-amber-300 font-semibold shadow-2xl backdrop-blur-sm">
                    %s
                </div>
                """
                        .formatted(escapeHtml(message)),
                null);
    }

    private ResponseEntity<String> htmlResponse(String body, String hxTrigger) {
        ResponseEntity.BodyBuilder responseBuilder =
                ResponseEntity.ok().header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE);
        if (hxTrigger != null && !hxTrigger.isBlank()) {
            responseBuilder.header("HX-Trigger", hxTrigger);
        }
        return responseBuilder.body(body);
    }

    private void audit(HttpServletRequest request, String action, String subject, String details) {
        if (jobOperationsService == null) {
            return;
        }
        jobOperationsService.recordAudit(resolveActor(request), action, subject, details);
    }

    private String resolveActor(HttpServletRequest request) {
        if (request != null
                && request.getUserPrincipal() != null
                && request.getUserPrincipal().getName() != null) {
            return request.getUserPrincipal().getName();
        }
        if (request == null) {
            return "dashboard";
        }
        String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authHeader != null && authHeader.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
            try {
                String decoded = new String(
                        Base64.getDecoder()
                                .decode(authHeader.substring("Basic ".length()).trim()),
                        StandardCharsets.UTF_8);
                int separatorIndex = decoded.indexOf(':');
                return separatorIndex > 0 ? decoded.substring(0, separatorIndex) : "dashboard-basic";
            } catch (IllegalArgumentException ignored) {
                return "dashboard-basic";
            }
        }
        return "dashboard";
    }

    private String successMessage(String message) {
        return """
                <div class="pointer-events-auto mb-3 rounded-xl border border-emerald-500/30 bg-slate-900/95 px-4 py-3 text-center text-emerald-300 font-semibold shadow-2xl backdrop-blur-sm">
                    %s
                </div>
                """
                .formatted(escapeHtml(message));
    }

    private String getBadgeClass(String status) {
        return switch (status) {
            case "PENDING" -> "bg-blue-500/10 text-blue-400 border-blue-500/20";
            case "PROCESSING" -> "bg-amber-500/10 text-amber-400 border-amber-500/20";
            case "COMPLETED" -> "bg-emerald-500/10 text-emerald-400 border-emerald-500/20";
            case "FAILED" -> "bg-rose-500/10 text-rose-400 border-rose-500/20";
            case "CANCELLED" -> "bg-slate-700/70 text-slate-300 border-slate-600";
            default -> "bg-slate-800 text-slate-400 border-slate-700";
        };
    }

    private String escapeHtml(String input) {
        if (input == null) return null;
        return input.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    private String formatPayload(JsonNode payload) {
        if (payload == null) return "null";
        try {
            return escapeHtml(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            return escapeHtml(payload.toString());
        }
    }

    private String restartButtonHtml(Job job) {
        String status = job.getStatus();
        OffsetDateTime now = OffsetDateTime.now();
        if (isDelayedPending(
                job.getProcessingStartedAt(),
                job.getFinishedAt(),
                job.getFailedAt(),
                job.getCancelledAt(),
                job.getRunAt(),
                now)) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <div class="grid grid-cols-2 gap-3">
                            <button class="w-full px-4 py-3 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                    hx-post="/jobq/htmx/job/%s/run-now" hx-target="#jobq-action-feedback" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator"
                                    hx-on:afterRequest="document.getElementById('modal-container').innerHTML=''">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M13 10V3L4 14h7v7l9-11h-7z'></path></svg>
                                Run Now
                            </button>
                            <button class="w-full px-4 py-3 bg-slate-700 hover:bg-slate-600 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                    hx-post="/jobq/htmx/job/%s/cancel" hx-target="#jobq-action-feedback" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator"
                                    hx-on:afterRequest="document.getElementById('modal-container').innerHTML=''">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M6 18L18 6M6 6l12 12'></path></svg>
                                Cancel
                            </button>
                        </div>
                    </div>
                    """
                    .formatted(job.getId().toString(), job.getId().toString());
        }
        if ("PENDING".equals(status) || "PROCESSING".equals(status)) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <button class="w-full px-4 py-3 bg-slate-700 hover:bg-slate-600 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                hx-post="/jobq/htmx/job/%s/cancel" hx-target="#jobq-action-feedback" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator"
                                hx-on:afterRequest="document.getElementById('modal-container').innerHTML=''">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M6 18L18 6M6 6l12 12'></path></svg>
                            Cancel Job
                        </button>
                    </div>
                    """
                    .formatted(job.getId().toString());
        }
        if ("FAILED".equals(status) || "COMPLETED".equals(status) || "CANCELLED".equals(status)) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <button class="w-full px-4 py-3 %s text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                hx-post="/jobq/htmx/job/%s/rerun" hx-target="#jobq-action-feedback" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator"
                                hx-on:afterRequest="document.getElementById('modal-container').innerHTML=''">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                            %s
                        </button>
                    </div>
                    """
                    .formatted(
                            "FAILED".equals(status)
                                    ? "bg-indigo-600 hover:bg-indigo-500"
                                    : "CANCELLED".equals(status)
                                            ? "bg-slate-600 hover:bg-slate-500"
                                            : "bg-emerald-600 hover:bg-emerald-500",
                            job.getId().toString(),
                            "FAILED".equals(status) ? "Retry Job" : "Rerun Job");
        }
        return "";
    }

    private String rowActionButtonHtml(String status, UUID id, OffsetDateTime runAt, OffsetDateTime now) {
        if ("PENDING".equals(status) && runAt != null && runAt.isAfter(now)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-blue-500/30 bg-blue-500/10 text-blue-300 text-xs font-semibold hover:bg-blue-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/run-now"
                            hx-target="#jobq-action-feedback"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Run now
                    </button>
                    """
                    .formatted(id.toString());
        }
        if ("PENDING".equals(status) || "PROCESSING".equals(status)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-slate-500/30 bg-slate-500/10 text-slate-200 text-xs font-semibold hover:bg-slate-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/cancel"
                            hx-target="#jobq-action-feedback"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Cancel
                    </button>
                    """
                    .formatted(id.toString());
        }
        if ("FAILED".equals(status)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-rose-500/30 bg-rose-500/10 text-rose-300 text-xs font-semibold hover:bg-rose-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/rerun"
                            hx-target="#jobq-action-feedback"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Retry
                    </button>
                    """
                    .formatted(id.toString());
        }
        if ("COMPLETED".equals(status) || "CANCELLED".equals(status)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border %s text-xs font-semibold transition-colors"
                            hx-post="/jobq/htmx/job/%s/rerun"
                            hx-target="#jobq-action-feedback"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Rerun
                    </button>
                    """
                    .formatted(
                            "COMPLETED".equals(status)
                                    ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300 hover:bg-emerald-500/20"
                                    : "border-slate-500/30 bg-slate-500/10 text-slate-200 hover:bg-slate-500/20",
                            id.toString());
        }
        return "";
    }

    private boolean isDelayedPending(
            OffsetDateTime processingStartedAt,
            OffsetDateTime finishedAt,
            OffsetDateTime failedAt,
            OffsetDateTime cancelledAt,
            OffsetDateTime runAt,
            OffsetDateTime now) {
        return processingStartedAt == null
                && finishedAt == null
                && failedAt == null
                && cancelledAt == null
                && runAt != null
                && runAt.isAfter(now);
    }

    private String formatFailedInfo(JobRepository.DashboardJobView job) {
        return formatFailedInfo(job.getFailedAt(), job.getCancelledAt(), job.getRetryCount(), job.getErrorMessage());
    }

    private String formatFailedInfo(
            OffsetDateTime failedAt, OffsetDateTime cancelledAt, int retryCount, String errorMessage) {
        if (failedAt == null && cancelledAt == null) {
            if (retryCount <= 0) {
                return "<span class=\"text-slate-500\">—</span>";
            }
            return "<span class=\"text-amber-300\">Retries used: " + retryCount + "</span>";
        }
        if (cancelledAt != null) {
            return """
                    <div class="space-y-1">
                        <div class="text-slate-300">Cancelled at %s</div>
                        <div class="text-slate-400 text-xs leading-5 break-words">%s</div>
                    </div>
                    """
                    .formatted(
                            cancelledAt.format(FORMATTER),
                            errorMessage == null ? "Cancelled from dashboard" : escapeHtml(shorten(errorMessage, 120)));
        }
        String failedAtText = failedAt.format(FORMATTER);
        if (errorMessage == null || errorMessage.isBlank()) {
            return "<div class=\"text-rose-300\">Failed at " + failedAtText + "</div>";
        }
        String escapedError = escapeHtml(errorMessage);
        String shortened = escapeHtml(shorten(errorMessage, 120));
        return """
                <div class="space-y-1">
                    <div class="text-rose-300">Failed at %s</div>
                    <div class="text-rose-200/90 text-xs leading-5 break-words" title="%s">%s</div>
                </div>
                """
                .formatted(failedAtText, escapedError, shortened);
    }

    private String detailErrorMessage(String errorMessage, boolean cancelled) {
        if (errorMessage == null || errorMessage.isBlank()) {
            return "";
        }
        return """
                <div>
                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">%s</dt>
                    <dd>
                        <pre class="bg-slate-950/80 border border-slate-800 rounded-lg p-4 text-xs leading-6 overflow-auto max-h-48 %s">%s</pre>
                    </dd>
                </div>
                """
                .formatted(
                        cancelled ? "Cancellation Reason" : "Failure",
                        cancelled ? "text-amber-100" : "text-rose-100",
                        escapeHtml(errorMessage));
    }

    private Slice<JobRepository.DashboardJobView> loadDashboardJobsSlice(
            String normalizedStatus,
            int page,
            int size,
            String normalizedQuery,
            String normalizedSort,
            boolean scheduledOnly,
            boolean retriedOnly) {
        PageRequest pageRequest = PageRequest.of(page, size, resolveSort(normalizedSort));
        UUID queriedJobId = parseUuid(normalizedQuery);
        boolean jobIdProvided = queriedJobId != null;
        return jobRepository.findDashboardJobViews(
                normalizedStatus,
                normalizedQuery,
                scheduledOnly,
                retriedOnly,
                jobIdProvided,
                jobIdProvided ? queriedJobId : ZERO_UUID,
                pageRequest);
    }

    private String formatTimeline(JobRepository.DashboardJobView job, String statusLabel, OffsetDateTime now) {
        OffsetDateTime createdAt = job.getCreatedAt();
        String createdAtText = createdAt != null ? createdAt.format(FORMATTER) : "—";

        if ("PENDING".equals(statusLabel)
                && job.getRunAt() != null
                && job.getRunAt().isAfter(now)) {
            return """
                    <div class="space-y-1">
                        <div>Created %s</div>
                        <div class="text-indigo-300 text-xs">Runs %s</div>
                    </div>
                    """
                    .formatted(createdAtText, job.getRunAt().format(FORMATTER));
        }

        if ("PROCESSING".equals(statusLabel) && job.getProcessingStartedAt() != null) {
            return """
                    <div class="space-y-1">
                        <div>Created %s</div>
                        <div class="text-amber-300 text-xs">Started %s</div>
                        <div class="text-slate-400 text-xs">%s</div>
                    </div>
                    """
                    .formatted(
                            createdAtText,
                            job.getProcessingStartedAt().format(FORMATTER),
                            job.getProgressMessage() != null
                                            && !job.getProgressMessage().isBlank()
                                    ? escapeHtml(shorten(job.getProgressMessage(), 60))
                                    : (job.getProgressPercent() != null
                                            ? job.getProgressPercent() + "% complete"
                                            : "Running"));
        }

        if ("COMPLETED".equals(statusLabel) && job.getFinishedAt() != null) {
            return """
                    <div class="space-y-1">
                        <div>Created %s</div>
                        <div class="text-emerald-300 text-xs">Finished %s</div>
                    </div>
                    """
                    .formatted(createdAtText, job.getFinishedAt().format(FORMATTER));
        }

        if ("FAILED".equals(statusLabel) && job.getFailedAt() != null) {
            return """
                    <div class="space-y-1">
                        <div>Created %s</div>
                        <div class="text-rose-300 text-xs">Failed %s</div>
                    </div>
                    """
                    .formatted(createdAtText, job.getFailedAt().format(FORMATTER));
        }

        if ("CANCELLED".equals(statusLabel) && job.getCancelledAt() != null) {
            return """
                    <div class="space-y-1">
                        <div>Created %s</div>
                        <div class="text-slate-300 text-xs">Cancelled %s</div>
                    </div>
                    """
                    .formatted(createdAtText, job.getCancelledAt().format(FORMATTER));
        }

        return createdAtText;
    }

    private String resolveStatus(JobRepository.DashboardJobView job) {
        if (job.getFinishedAt() != null) {
            return "COMPLETED";
        }
        if (job.getFailedAt() != null) {
            return "FAILED";
        }
        if (job.getCancelledAt() != null) {
            return "CANCELLED";
        }
        if (job.getProcessingStartedAt() != null) {
            return "PROCESSING";
        }
        return "PENDING";
    }

    private Comparator<JobOperationsService.QueueStats> queueStatsComparator() {
        return Comparator.comparing(JobOperationsService.QueueStats::activeBacklog)
                .reversed()
                .thenComparing(JobOperationsService.QueueStats::totalCount, Comparator.reverseOrder())
                .thenComparing(JobOperationsService.QueueStats::hasControls, Comparator.reverseOrder())
                .thenComparing(JobOperationsService.QueueStats::recurring, Comparator.reverseOrder())
                .thenComparing(JobOperationsService.QueueStats::type, String.CASE_INSENSITIVE_ORDER);
    }

    private String renderPaginationControls(String normalizedStatus, int normalizedPage, boolean hasNext) {
        int previousPage = Math.max(0, normalizedPage - 1);
        int nextPage = normalizedPage + 1;
        String escapedStatus = escapeJs(normalizedStatus);
        return """
                <div class="flex items-center gap-3 text-sm text-slate-400">
                    <span>Page %d%s</span>
                    <div class="flex gap-1">
                        <button class="p-1.5 rounded bg-slate-800 border border-slate-700 hover:bg-slate-700 disabled:opacity-50 transition"
                                %s
                                hx-on:click="document.querySelectorAll('input[name=status]').forEach(e=>e.value='%s');document.querySelectorAll('select[name=statusFilter]').forEach(e=>e.value='%s');document.querySelectorAll('input[name=page]').forEach(e=>e.value='%d');htmx.trigger(document.body,'jobq-refresh')">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path></svg>
                        </button>
                        <button class="p-1.5 rounded bg-slate-800 border border-slate-700 hover:bg-slate-700 disabled:opacity-50 transition"
                                %s
                                hx-on:click="document.querySelectorAll('input[name=status]').forEach(e=>e.value='%s');document.querySelectorAll('select[name=statusFilter]').forEach(e=>e.value='%s');document.querySelectorAll('input[name=page]').forEach(e=>e.value='%d');htmx.trigger(document.body,'jobq-refresh')">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path></svg>
                        </button>
                    </div>
                </div>
                """
                .formatted(
                        normalizedPage + 1,
                        hasNext ? "+" : "",
                        normalizedPage == 0 ? "disabled" : "",
                        escapedStatus,
                        escapedStatus,
                        previousPage,
                        !hasNext ? "disabled" : "",
                        escapedStatus,
                        escapedStatus,
                        nextPage);
    }

    private String detailCard(String label, String valueHtml) {
        return """
                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">%s</dt>
                    <dd class="mt-1 text-sm text-slate-200 break-words">%s</dd>
                </div>
                """
                .formatted(escapeHtml(label), valueHtml == null ? "—" : valueHtml);
    }

    private String metricCard(String label, String value) {
        return """
                <div class="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
                    <div class="text-[11px] uppercase tracking-wider text-slate-500">%s</div>
                    <div class="mt-1 text-lg font-semibold text-slate-100">%s</div>
                </div>
                """
                .formatted(escapeHtml(label), escapeHtml(value));
    }

    private String emptyPanelState(String message) {
        return """
                <tr>
                    <td colspan="8" class="px-4 py-8 text-center text-slate-500">%s</td>
                </tr>
                """
                .formatted(escapeHtml(message));
    }

    private String logLevelClass(String level) {
        if (level == null) {
            return "bg-slate-700 text-slate-200";
        }
        return switch (level.toUpperCase(Locale.ROOT)) {
            case "ERROR" -> "bg-rose-500/10 text-rose-300";
            case "WARN" -> "bg-amber-500/10 text-amber-300";
            case "DEBUG", "TRACE" -> "bg-indigo-500/10 text-indigo-300";
            default -> "bg-emerald-500/10 text-emerald-300";
        };
    }

    private String formatExactTimestamp(OffsetDateTime timestamp) {
        return timestamp == null ? "—" : timestamp.format(EXACT_FORMATTER);
    }

    private String normalizeQuery(String rawQuery) {
        if (rawQuery == null) {
            return "";
        }
        String normalized = rawQuery.trim();
        if (normalized.length() > MAX_QUERY_LENGTH) {
            normalized = normalized.substring(0, MAX_QUERY_LENGTH);
        }
        return normalized;
    }

    private List<UUID> parseSelectedIds(String rawSelectedIds) {
        if (rawSelectedIds == null || rawSelectedIds.isBlank()) {
            return List.of();
        }
        LinkedHashSet<UUID> uniqueIds = new LinkedHashSet<>();
        String[] parts = rawSelectedIds.split("[,\\s]+");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            try {
                uniqueIds.add(UUID.fromString(part.trim()));
            } catch (IllegalArgumentException ignored) {
                // Ignore malformed ids from the client.
            }
            if (uniqueIds.size() >= MAX_BATCH_IDS) {
                break;
            }
        }
        return new ArrayList<>(uniqueIds);
    }

    private OffsetDateTime parseFailedSince(String rawFailedSince) {
        if (rawFailedSince == null || rawFailedSince.isBlank()) {
            return null;
        }
        String normalized = rawFailedSince.trim();
        try {
            return OffsetDateTime.parse(normalized);
        } catch (DateTimeParseException ignored) {
            // Fall through and attempt local date-time parsing.
        }
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(normalized);
            return localDateTime.atZone(ZoneId.systemDefault()).toOffsetDateTime();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private String normalizeSort(String rawSort) {
        if (rawSort == null) {
            return "created-desc";
        }
        return switch (rawSort) {
            case "created-asc",
                    "priority-desc",
                    "priority-asc",
                    "retry-desc",
                    "type-asc",
                    "type-desc",
                    "failed-desc",
                    "run-at-asc",
                    "run-at-desc" -> rawSort;
            default -> "created-desc";
        };
    }

    private Sort resolveSort(String normalizedSort) {
        return switch (normalizedSort) {
            case "created-asc" -> Sort.by(Sort.Order.asc("createdAt"), Sort.Order.asc("id"));
            case "priority-desc" -> Sort.by(Sort.Order.desc("priority"), Sort.Order.desc("createdAt"));
            case "priority-asc" -> Sort.by(Sort.Order.asc("priority"), Sort.Order.desc("createdAt"));
            case "retry-desc" -> Sort.by(Sort.Order.desc("retryCount"), Sort.Order.desc("createdAt"));
            case "type-asc" -> Sort.by(Sort.Order.asc("type"), Sort.Order.desc("createdAt"));
            case "type-desc" -> Sort.by(Sort.Order.desc("type"), Sort.Order.desc("createdAt"));
            case "failed-desc" ->
                Sort.by(Sort.Order.desc("failedAt").nullsLast(), Sort.Order.desc("createdAt"), Sort.Order.desc("id"));
            case "run-at-asc" -> Sort.by(Sort.Order.asc("runAt"), Sort.Order.desc("createdAt"), Sort.Order.asc("id"));
            case "run-at-desc" ->
                Sort.by(Sort.Order.desc("runAt"), Sort.Order.desc("createdAt"), Sort.Order.desc("id"));
            default -> Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("id"));
        };
    }

    private UUID parseUuid(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value.trim());
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private String normalizeStatus(String rawStatus) {
        if (rawStatus == null) {
            return "";
        }
        String normalized = rawStatus.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "PENDING", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED" -> normalized;
            default -> "";
        };
    }

    private long countOrZero(Long value) {
        return value == null ? 0L : value;
    }

    private String escapeJs(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\").replace("'", "\\'");
    }

    private String shorten(String input, int maxLength) {
        if (input == null || input.length() <= maxLength) {
            return input;
        }
        return input.substring(0, Math.max(0, maxLength - 3)) + "...";
    }
}
