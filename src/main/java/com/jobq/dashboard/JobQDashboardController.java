package com.jobq.dashboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
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
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("MMM dd HH:mm:ss");
    private static final DateTimeFormatter EXACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final UUID ZERO_UUID = new UUID(0L, 0L);
    private static final int MAX_QUERY_LENGTH = 200;

    public JobQDashboardController(JobRepository jobRepository, ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
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
        long total = pending + processing + completed + failed;

        return """
                <div class="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
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
                                "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'></path></svg>"));
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
                              <td colspan="9" class="px-6 py-12 text-center text-slate-400">
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
                                    hx-get="/jobq/htmx/job/%s" hx-target="#modal-container" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator">
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

    @GetMapping(value = "/job/{id}", produces = MediaType.TEXT_HTML_VALUE)
    public String getJobDetails(@PathVariable("id") UUID id) {
        return jobRepository
                .findById(id)
                .map(job -> {
                    String statusLabel = job.getStatus();
                    String errorHtml = job.getErrorMessage() != null
                            ? """
                            <hr class="border-slate-800 my-4">
                            <div class="bg-rose-500/10 p-4 rounded-md border border-rose-500/20">
                                <dt class="text-xs font-semibold uppercase tracking-wider text-rose-400 flex items-center gap-2 mb-2">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
                                    Last Error Message
                                </dt>
                                <dd class="text-sm text-rose-300 font-mono whitespace-pre-wrap break-words max-h-48 overflow-y-auto">%s</dd>
                            </div>
                            """
                                    .formatted(escapeHtml(job.getErrorMessage()))
                            : "";

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
                                        <dl class="space-y-6">
                                            <div class="grid grid-cols-2 gap-6">
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Type</dt>
                                                    <dd class="mt-1 text-sm font-medium text-slate-200">%s</dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Priority</dt>
                                                    <dd class="mt-1 text-sm font-medium text-slate-200">%d</dd>
                                                </div>
                                            </div>
                                            <div class="grid grid-cols-2 gap-6">
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Retries</dt>
                                                    <dd class="mt-1 text-sm font-medium %s">%d <span class="text-slate-500">/ %d</span></dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Locked By</dt>
                                                    <dd class="mt-1 text-sm text-slate-300 break-all">%s</dd>
                                                </div>
                                            </div>
                                            <div class="grid grid-cols-2 gap-6">
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Created At</dt>
                                                    <dd class="mt-1 text-xs font-mono text-slate-300">%s</dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Run At</dt>
                                                    <dd class="mt-1 text-xs font-mono text-slate-300">%s</dd>
                                                </div>
                                            </div>
                                            <div class="grid grid-cols-3 gap-6">
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Processing Started At</dt>
                                                    <dd class="mt-1 text-xs font-mono text-slate-300">%s</dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Finished At</dt>
                                                    <dd class="mt-1 text-xs font-mono text-slate-300">%s</dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Failed At</dt>
                                                    <dd class="mt-1 text-xs font-mono text-slate-300">%s</dd>
                                                </div>
                                            </div>
                                            <div class="grid grid-cols-2 gap-6">
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Group ID</dt>
                                                    <dd class="mt-1 text-sm font-medium text-slate-200">%s</dd>
                                                </div>
                                                <div class="bg-slate-800/50 p-4 rounded-lg border border-slate-800">
                                                    <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500">Replace Key</dt>
                                                    <dd class="mt-1 text-sm font-medium text-slate-200">%s</dd>
                                                </div>
                                            </div>
                                            %s
                                            <div>
                                                <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Payload (JSON)</dt>
                                                <dd>
                                                    <pre class="bg-[#0d1117] text-[#c9d1d9] p-4 rounded-lg text-xs font-mono overflow-auto max-h-96 shadow-inner border border-slate-800">%s</pre>
                                                </dd>
                                            </div>
                                            %s
                                        </dl>
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
                                    escapeHtml(job.getType()),
                                    job.getPriority(),
                                    job.getRetryCount() > 0 ? "text-rose-400" : "text-slate-200",
                                    job.getRetryCount(),
                                    job.getMaxRetries(),
                                    job.getLockedBy() != null ? job.getLockedBy() : "—",
                                    job.getCreatedAt() != null
                                            ? job.getCreatedAt().format(EXACT_FORMATTER)
                                            : "—",
                                    job.getRunAt() != null ? job.getRunAt().format(EXACT_FORMATTER) : "—",
                                    job.getProcessingStartedAt() != null
                                            ? job.getProcessingStartedAt().format(EXACT_FORMATTER)
                                            : "—",
                                    job.getFinishedAt() != null
                                            ? job.getFinishedAt().format(EXACT_FORMATTER)
                                            : "—",
                                    job.getFailedAt() != null
                                            ? job.getFailedAt().format(EXACT_FORMATTER)
                                            : "—",
                                    job.getGroupId() != null ? escapeHtml(job.getGroupId()) : "—",
                                    job.getReplaceKey() != null ? escapeHtml(job.getReplaceKey()) : "—",
                                    errorHtml,
                                    formatPayload(job.getPayload()),
                                    restartButtonHtml(job));
                })
                .orElse("<div class='p-4 text-red-500'>Job not found.</div>");
    }

    @PostMapping(
            value = {"/job/{id}/restart", "/job/{id}/retry", "/job/{id}/rerun"},
            produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> rerunJob(@PathVariable("id") UUID id) {
        String body = jobRepository
                .findById(id)
                .map(job -> {
                    boolean failed = job.getFailedAt() != null;
                    boolean completed = job.getFinishedAt() != null;
                    if (!failed && !completed) {
                        return """
                        <div class="p-4 text-center text-amber-300 font-semibold">
                            Job %s is not in COMPLETED or FAILED state and cannot be rerun.
                        </div>
                        """
                                .formatted(id.toString().substring(0, 8));
                    }

                    job.setProcessingStartedAt(null);
                    job.setFinishedAt(null);
                    job.setFailedAt(null);
                    job.setRetryCount(0);
                    job.setErrorMessage(null);
                    job.setLockedAt(null);
                    job.setLockedBy(null);
                    job.setRunAt(java.time.OffsetDateTime.now());
                    job.setUpdatedAt(java.time.OffsetDateTime.now());
                    jobRepository.save(job);
                    return """
                    <div class="p-4 text-center text-emerald-400 font-semibold">
                        ✅ Job %s has been queued for %s. It will be picked up on the next poll cycle.
                    </div>
                    """
                            .formatted(id.toString().substring(0, 8), failed ? "retry" : "rerun");
                })
                .orElse("<div class='p-4 text-red-500'>Job not found.</div>");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
                .header("HX-Trigger", "jobq-refresh")
                .body(body);
    }

    @PostMapping(value = "/job/{id}/run-now", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> runJobNow(@PathVariable("id") UUID id) {
        OffsetDateTime now = OffsetDateTime.now();
        String body = jobRepository
                .findById(id)
                .map(job -> {
                    if (!isDelayedPending(
                            job.getProcessingStartedAt(),
                            job.getFinishedAt(),
                            job.getFailedAt(),
                            job.getRunAt(),
                            now)) {
                        return """
                        <div class="p-4 text-center text-amber-300 font-semibold">
                            Job %s is not delayed in PENDING state and cannot be forced to run now.
                        </div>
                        """
                                .formatted(id.toString().substring(0, 8));
                    }
                    job.setRunAt(now);
                    job.setUpdatedAt(now);
                    jobRepository.save(job);
                    return """
                    <div class="p-4 text-center text-emerald-400 font-semibold">
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

    private String getBadgeClass(String status) {
        return switch (status) {
            case "PENDING" -> "bg-blue-500/10 text-blue-400 border-blue-500/20";
            case "PROCESSING" -> "bg-amber-500/10 text-amber-400 border-amber-500/20";
            case "COMPLETED" -> "bg-emerald-500/10 text-emerald-400 border-emerald-500/20";
            case "FAILED" -> "bg-rose-500/10 text-rose-400 border-rose-500/20";
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
        OffsetDateTime now = OffsetDateTime.now();
        if (isDelayedPending(
                job.getProcessingStartedAt(), job.getFinishedAt(), job.getFailedAt(), job.getRunAt(), now)) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <button class="w-full px-4 py-3 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                hx-post="/jobq/htmx/job/%s/run-now" hx-target="#modal-container" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M13 10V3L4 14h7v7l9-11h-7z'></path></svg>
                            Run Now
                        </button>
                    </div>
                    """
                    .formatted(job.getId().toString());
        }
        if ("FAILED".equals(job.getStatus())) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <button class="w-full px-4 py-3 bg-indigo-600 hover:bg-indigo-500 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                hx-post="/jobq/htmx/job/%s/rerun" hx-target="#modal-container" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                            Retry Job
                        </button>
                    </div>
                    """
                    .formatted(job.getId().toString());
        }
        if ("COMPLETED".equals(job.getStatus())) {
            return """
                    <div class="mt-6 pt-6 border-t border-slate-800">
                        <button class="w-full px-4 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
                                hx-post="/jobq/htmx/job/%s/rerun" hx-target="#modal-container" hx-swap="innerHTML" hx-indicator="#jobq-loading-indicator">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                            Rerun Job
                        </button>
                    </div>
                    """
                    .formatted(job.getId().toString());
        }
        return "";
    }

    private String rowActionButtonHtml(String status, UUID id, OffsetDateTime runAt, OffsetDateTime now) {
        if ("PENDING".equals(status) && runAt != null && runAt.isAfter(now)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-blue-500/30 bg-blue-500/10 text-blue-300 text-xs font-semibold hover:bg-blue-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/run-now"
                            hx-target="#modal-container"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Run now
                    </button>
                    """
                    .formatted(id.toString());
        }
        if ("FAILED".equals(status)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-rose-500/30 bg-rose-500/10 text-rose-300 text-xs font-semibold hover:bg-rose-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/rerun"
                            hx-target="#modal-container"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Retry
                    </button>
                    """
                    .formatted(id.toString());
        }
        if ("COMPLETED".equals(status)) {
            return """
                    <button class="px-2.5 py-1 rounded-md border border-emerald-500/30 bg-emerald-500/10 text-emerald-300 text-xs font-semibold hover:bg-emerald-500/20 transition-colors"
                            hx-post="/jobq/htmx/job/%s/rerun"
                            hx-target="#modal-container"
                            hx-swap="innerHTML"
                            hx-indicator="#jobq-loading-indicator"
                            hx-on:click="event.stopPropagation()">
                        Rerun
                    </button>
                    """
                    .formatted(id.toString());
        }
        return "";
    }

    private boolean isDelayedPending(
            OffsetDateTime processingStartedAt,
            OffsetDateTime finishedAt,
            OffsetDateTime failedAt,
            OffsetDateTime runAt,
            OffsetDateTime now) {
        return processingStartedAt == null
                && finishedAt == null
                && failedAt == null
                && runAt != null
                && runAt.isAfter(now);
    }

    private String formatFailedInfo(JobRepository.DashboardJobView job) {
        return formatFailedInfo(job.getFailedAt(), job.getRetryCount(), job.getErrorMessage());
    }

    private String formatFailedInfo(OffsetDateTime failedAt, int retryCount, String errorMessage) {
        if (failedAt == null) {
            if (retryCount <= 0) {
                return "<span class=\"text-slate-500\">—</span>";
            }
            return "<span class=\"text-amber-300\">Retries used: " + retryCount + "</span>";
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
                    </div>
                    """
                    .formatted(createdAtText, job.getProcessingStartedAt().format(FORMATTER));
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

        return createdAtText;
    }

    private String resolveStatus(JobRepository.DashboardJobView job) {
        if (job.getFinishedAt() != null) {
            return "COMPLETED";
        }
        if (job.getFailedAt() != null) {
            return "FAILED";
        }
        if (job.getProcessingStartedAt() != null) {
            return "PROCESSING";
        }
        return "PENDING";
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
        return switch (rawStatus) {
            case "PENDING", "PROCESSING", "COMPLETED", "FAILED" -> rawStatus;
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
