package com.jobq.dashboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobq.Job;
import com.jobq.JobRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jobq/htmx")
@ConditionalOnProperty(prefix = "jobq.dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobQDashboardController {

    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("MMM dd HH:mm:ss");
    private static final DateTimeFormatter EXACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public JobQDashboardController(JobRepository jobRepository, ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
    }

    @GetMapping(value = "/stats", produces = MediaType.TEXT_HTML_VALUE)
    public String getStats(@RequestParam(name = "filter", required = false, defaultValue = "") String filter) {
        long pending = 0;
        long processing = 0;
        long completed = 0;
        long failed = 0;
        long total = jobRepository.count();

        List<Job> allJobs = jobRepository.findAll();
        for (Job job : allJobs) {
            switch (job.getStatus()) {
                case "PENDING" -> pending++;
                case "PROCESSING" -> processing++;
                case "COMPLETED" -> completed++;
                case "FAILED" -> failed++;
            }
        }

        return """
                <div class="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
                    %s
                    %s
                    %s
                    %s
                    %s
                </div>
                """.formatted(
                statCard("Total Jobs", total, "", filter, "ring-indigo-500", "text-slate-500", ""),
                statCard("Pending", pending, "PENDING", filter, "ring-blue-500", "text-blue-500",
                        "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z'></path></svg>"),
                statCard("Processing", processing, "PROCESSING", filter, "ring-amber-500", "text-amber-500",
                        "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15'></path></svg>"),
                statCard("Completed", completed, "COMPLETED", filter, "ring-emerald-500", "text-emerald-500",
                        "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M5 13l4 4L19 7'></path></svg>"),
                statCard("Failed", failed, "FAILED", filter, "ring-rose-500", "text-rose-500",
                        "<svg class='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'></path></svg>"));
    }

    private String statCard(String title, long value, String targetFilter, String activeFilter, String ringClass,
            String textClass, String iconHtml) {
        String activeState = activeFilter.equals(targetFilter) ? "ring-2 " + ringClass : "";
        return """
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 cursor-pointer hover:-translate-y-1 hover:shadow-lg hover:shadow-indigo-500/10 transition-all duration-200 %s"
                     hx-get="/jobq/htmx/jobs?status=%s&page=0"
                     hx-target="#jobs-container"
                     hx-on::after-request="document.querySelectorAll('input[name=status]').forEach(e=>e.value='%s')">
                    <div class="%s text-xs font-semibold uppercase tracking-wider mb-2 flex items-center gap-1.5 opacity-80">
                        %s %s
                    </div>
                    <div class="text-3xl font-bold text-slate-100">%d</div>
                </div>
                """
                .formatted(activeState, targetFilter, targetFilter, textClass, iconHtml, title, value);
    }

    @GetMapping(value = "/jobs", produces = MediaType.TEXT_HTML_VALUE)
    public String getJobs(
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "50") int size,
            @RequestParam(name = "status", required = false, defaultValue = "") String status) {

        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Job> jobsPage = (!status.isBlank())
                ? jobRepository.findByStatus(status, pageRequest)
                : jobRepository.findAll(pageRequest);

        StringBuilder rows = new StringBuilder();

        if (jobsPage.isEmpty()) {
            rows.append(
                    """
                            <tr>
                              <td colspan="6" class="px-6 py-12 text-center text-slate-400">
                                <div class="flex flex-col items-center justify-center">
                                  <svg class="w-10 h-10 mb-3 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"></path></svg>
                                  <span class="text-lg font-medium">No jobs found %s</span>
                                </div>
                              </td>
                            </tr>
                            """
                            .formatted(!status.isBlank() ? "in " + status : ""));
        } else {
            for (Job job : jobsPage.getContent()) {
                rows.append(
                        """
                                <tr class="hover:bg-slate-800/50 transition-colors border-b border-slate-800/50 group cursor-pointer"
                                    hx-get="/jobq/htmx/job/%s" hx-target="#modal-container" hx-swap="innerHTML">
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
                                    <td class="px-6 py-3 font-mono text-xs text-slate-500">%s</td>
                                    <td class="px-6 py-3 text-right">
                                       <span class="text-indigo-400 text-xs font-semibold opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-end gap-1">
                                         Details <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path></svg>
                                       </span>
                                    </td>
                                </tr>
                                """
                                .formatted(
                                        job.getId().toString(),
                                        getBadgeClass(job.getStatus()), job.getStatus(),
                                        escapeHtml(job.getType()),
                                        job.getRetryCount() > 0
                                                ? "bg-rose-500/10 text-rose-400 border border-rose-500/20"
                                                : "bg-slate-800 text-slate-400",
                                        job.getRetryCount(), job.getMaxRetries(), job.getPriority(),
                                        job.getCreatedAt() != null ? job.getCreatedAt().format(FORMATTER) : "—",
                                        job.getId().toString().substring(0, 8) + "..."));
            }
        }

        // Return the actual table body along with Out-of-Band updates for pagination
        // and triggers for stats
        return """
                %s
                <div id="pagination-controls" hx-swap-oob="true" class="flex items-center gap-3 text-sm text-slate-400">
                    <span>Page %d of %d</span>
                    <div class="flex gap-1">
                        <button class="p-1.5 rounded bg-slate-800 border border-slate-700 hover:bg-slate-700 disabled:opacity-50 transition"
                                %s hx-get="/jobq/htmx/jobs?status=%s&page=%d" hx-target="#jobs-container">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path></svg>
                        </button>
                        <button class="p-1.5 rounded bg-slate-800 border border-slate-700 hover:bg-slate-700 disabled:opacity-50 transition"
                                %s hx-get="/jobq/htmx/jobs?status=%s&page=%d" hx-target="#jobs-container">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path></svg>
                        </button>
                    </div>
                </div>
                <div id="stats-trigger" hx-swap-oob="true" hx-get="/jobq/htmx/stats?filter=%s" hx-trigger="load" hx-target="#stats-container"></div>
                """
                .formatted(
                        rows.toString(),
                        page + 1, Math.max(1, jobsPage.getTotalPages()),
                        page == 0 ? "disabled" : "", status, Math.max(0, page - 1),
                        page >= jobsPage.getTotalPages() - 1 ? "disabled" : "", status, page + 1,
                        status);
    }

    @GetMapping(value = "/job/{id}", produces = MediaType.TEXT_HTML_VALUE)
    public String getJobDetails(@PathVariable("id") UUID id) {
        return jobRepository.findById(id).map(job -> {
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
                                            %s
                                            <div>
                                                <dt class="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Payload (JSON)</dt>
                                                <dd>
                                                    <pre class="bg-[#0d1117] text-[#c9d1d9] p-4 rounded-lg text-xs font-mono overflow-auto max-h-96 shadow-inner border border-slate-800">%s</pre>
                                                </dd>
                                            </div>
                                        </dl>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """
                    .formatted(
                            getBadgeClass(job.getStatus()), job.getStatus(),
                            job.getId().toString(),
                            escapeHtml(job.getType()),
                            job.getPriority(),
                            job.getRetryCount() > 0 ? "text-rose-400" : "text-slate-200", job.getRetryCount(),
                            job.getMaxRetries(),
                            job.getLockedBy() != null ? job.getLockedBy() : "—",
                            job.getCreatedAt() != null ? job.getCreatedAt().format(EXACT_FORMATTER) : "—",
                            job.getRunAt() != null ? job.getRunAt().format(EXACT_FORMATTER) : "—",
                            errorHtml,
                            formatPayload(job.getPayload()));
        }).orElse("<div class='p-4 text-red-500'>Job not found.</div>");
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
        if (input == null)
            return null;
        return input.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    private String formatPayload(JsonNode payload) {
        if (payload == null)
            return "null";
        try {
            return escapeHtml(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            return escapeHtml(payload.toString());
        }
    }
}
