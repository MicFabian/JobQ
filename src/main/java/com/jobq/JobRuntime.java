package com.jobq;

import com.jobq.dashboard.JobDashboardEventBus;
import com.jobq.internal.JobOperationsService;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class JobRuntime {

    private static final ThreadLocal<ExecutionContext> CURRENT = new ThreadLocal<>();

    private final JobRepository jobRepository;
    private final JobOperationsService jobOperationsService;
    private final JobDashboardEventBus dashboardEventBus;

    public JobRuntime(
            JobRepository jobRepository,
            JobOperationsService jobOperationsService,
            ObjectProvider<JobDashboardEventBus> dashboardEventBusProvider) {
        this.jobRepository = jobRepository;
        this.jobOperationsService = jobOperationsService;
        this.dashboardEventBus = dashboardEventBusProvider.getIfAvailable();
    }

    public JobExecutionHandle open(UUID jobId, String jobType, OffsetDateTime lockedAt, String lockedBy) {
        ExecutionContext previous = CURRENT.get();
        CURRENT.set(new ExecutionContext(jobId, jobType, lockedAt, lockedBy));
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    public boolean isActive() {
        return CURRENT.get() != null;
    }

    public UUID currentJobId() {
        ExecutionContext context = requireCurrentContext();
        return context.jobId();
    }

    public String currentJobType() {
        ExecutionContext context = requireCurrentContext();
        return context.jobType();
    }

    public void setProgress(int progressPercent) {
        setProgress(progressPercent, null);
    }

    public void setProgress(int progressPercent, String progressMessage) {
        int normalizedProgress = Math.max(0, Math.min(100, progressPercent));
        ExecutionContext context = requireCurrentContext();
        jobRepository.updateProgress(
                context.jobId(),
                context.lockedAt(),
                context.lockedBy(),
                normalizedProgress,
                normalizeMessage(progressMessage),
                OffsetDateTime.now());
        publishRuntimeUpdate(context.jobId());
    }

    public void logTrace(String message) {
        appendLog("TRACE", message);
    }

    public void logDebug(String message) {
        appendLog("DEBUG", message);
    }

    public void logInfo(String message) {
        appendLog("INFO", message);
    }

    public void logWarn(String message) {
        appendLog("WARN", message);
    }

    public void logError(String message) {
        appendLog("ERROR", message);
    }

    private void appendLog(String level, String message) {
        ExecutionContext context = requireCurrentContext();
        jobOperationsService.appendJobLog(context.jobId(), level, message);
        publishRuntimeUpdate(context.jobId());
    }

    private void publishRuntimeUpdate(UUID jobId) {
        if (dashboardEventBus != null) {
            dashboardEventBus.publishJobRuntime(jobId);
        }
    }

    private ExecutionContext requireCurrentContext() {
        ExecutionContext context = CURRENT.get();
        if (context == null) {
            throw new IllegalStateException("No JobQ runtime context is active on this thread");
        }
        return context;
    }

    private String normalizeMessage(String message) {
        if (message == null) {
            return null;
        }
        String normalized = message.strip();
        return normalized.isEmpty() ? null : normalized;
    }

    private record ExecutionContext(UUID jobId, String jobType, OffsetDateTime lockedAt, String lockedBy) {}

    @FunctionalInterface
    public interface JobExecutionHandle extends AutoCloseable {
        @Override
        void close();
    }
}
