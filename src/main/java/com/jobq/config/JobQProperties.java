package com.jobq.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "jobq")
public class JobQProperties {

    // Legacy polling properties replaced by BackgroundJobServer properties,
    // left below for backwards compatibility but we'll adapt to new ones if set.

    private final Database database = new Database();
    private final Jobs jobs = new Jobs();
    private final BackgroundJobServer backgroundJobServer = new BackgroundJobServer();
    private final Dashboard dashboard = new Dashboard();

    public Database getDatabase() {
        return database;
    }

    public Jobs getJobs() {
        return jobs;
    }

    public BackgroundJobServer getBackgroundJobServer() {
        return backgroundJobServer;
    }

    public Dashboard getDashboard() {
        return dashboard;
    }

    public static class Database {
        private String tablePrefix = "";
        private boolean skipCreate = false;
        private boolean failOnMigrationError = true;

        public String getTablePrefix() {
            return tablePrefix;
        }

        public void setTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
        }

        public boolean isSkipCreate() {
            return skipCreate;
        }

        public void setSkipCreate(boolean skipCreate) {
            this.skipCreate = skipCreate;
        }

        public boolean isFailOnMigrationError() {
            return failOnMigrationError;
        }

        public void setFailOnMigrationError(boolean failOnMigrationError) {
            this.failOnMigrationError = failOnMigrationError;
        }
    }

    public static class Jobs {
        private int defaultNumberOfRetries = 10;
        private int retryBackOffTimeSeed = 3;

        public int getDefaultNumberOfRetries() {
            return defaultNumberOfRetries;
        }

        public void setDefaultNumberOfRetries(int defaultNumberOfRetries) {
            this.defaultNumberOfRetries = defaultNumberOfRetries;
        }

        public int getRetryBackOffTimeSeed() {
            return retryBackOffTimeSeed;
        }

        public void setRetryBackOffTimeSeed(int retryBackOffTimeSeed) {
            this.retryBackOffTimeSeed = retryBackOffTimeSeed;
        }
    }

    public static class BackgroundJobServer {
        private boolean enabled = true;
        private int workerCount = Math.max(2, Runtime.getRuntime().availableProcessors());
        private boolean virtualThreadsEnabled = false;
        private long pollIntervalInSeconds = 15;
        private long recurringReconciliationIntervalInSeconds = 30;
        private boolean notifyEnabled = true;
        private String notifyChannel = "jobq_jobs_available";
        private int notifyListenTimeoutMs = 1_000;
        private long executionTimeoutCheckIntervalInSeconds = 30;
        private long nodeHeartbeatIntervalInSeconds = 10;
        private String deleteSucceededJobsAfter = "36h";
        private String permanentlyDeleteDeletedJobsAfter = "72h";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getWorkerCount() {
            return workerCount;
        }

        public void setWorkerCount(int workerCount) {
            this.workerCount = workerCount;
        }

        public boolean isVirtualThreadsEnabled() {
            return virtualThreadsEnabled;
        }

        public void setVirtualThreadsEnabled(boolean virtualThreadsEnabled) {
            this.virtualThreadsEnabled = virtualThreadsEnabled;
        }

        public long getPollIntervalInSeconds() {
            return pollIntervalInSeconds;
        }

        public void setPollIntervalInSeconds(long pollIntervalInSeconds) {
            this.pollIntervalInSeconds = pollIntervalInSeconds;
        }

        public long getRecurringReconciliationIntervalInSeconds() {
            return recurringReconciliationIntervalInSeconds;
        }

        public void setRecurringReconciliationIntervalInSeconds(long recurringReconciliationIntervalInSeconds) {
            this.recurringReconciliationIntervalInSeconds = recurringReconciliationIntervalInSeconds;
        }

        public boolean isNotifyEnabled() {
            return notifyEnabled;
        }

        public void setNotifyEnabled(boolean notifyEnabled) {
            this.notifyEnabled = notifyEnabled;
        }

        public String getNotifyChannel() {
            return notifyChannel;
        }

        public void setNotifyChannel(String notifyChannel) {
            this.notifyChannel = notifyChannel;
        }

        public int getNotifyListenTimeoutMs() {
            return notifyListenTimeoutMs;
        }

        public void setNotifyListenTimeoutMs(int notifyListenTimeoutMs) {
            this.notifyListenTimeoutMs = notifyListenTimeoutMs;
        }

        public long getExecutionTimeoutCheckIntervalInSeconds() {
            return executionTimeoutCheckIntervalInSeconds;
        }

        public void setExecutionTimeoutCheckIntervalInSeconds(long executionTimeoutCheckIntervalInSeconds) {
            this.executionTimeoutCheckIntervalInSeconds = executionTimeoutCheckIntervalInSeconds;
        }

        public long getNodeHeartbeatIntervalInSeconds() {
            return nodeHeartbeatIntervalInSeconds;
        }

        public void setNodeHeartbeatIntervalInSeconds(long nodeHeartbeatIntervalInSeconds) {
            this.nodeHeartbeatIntervalInSeconds = nodeHeartbeatIntervalInSeconds;
        }

        public String getDeleteSucceededJobsAfter() {
            return deleteSucceededJobsAfter;
        }

        public void setDeleteSucceededJobsAfter(String deleteSucceededJobsAfter) {
            this.deleteSucceededJobsAfter = deleteSucceededJobsAfter;
        }

        public String getPermanentlyDeleteDeletedJobsAfter() {
            return permanentlyDeleteDeletedJobsAfter;
        }

        public void setPermanentlyDeleteDeletedJobsAfter(String permanentlyDeleteDeletedJobsAfter) {
            this.permanentlyDeleteDeletedJobsAfter = permanentlyDeleteDeletedJobsAfter;
        }
    }

    public static class Dashboard {
        private boolean enabled = false;
        private String path = "/jobq/dashboard";
        private AuthMode authMode = AuthMode.BASIC;
        private String requiredRole = "JOBQ_DASHBOARD";
        private String username = "";
        private String password = "";
        private boolean readOnly = false;
        private List<String> redactedPayloadFields = new ArrayList<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public AuthMode getAuthMode() {
            return authMode;
        }

        public void setAuthMode(AuthMode authMode) {
            this.authMode = authMode;
        }

        public String getRequiredRole() {
            return requiredRole;
        }

        public void setRequiredRole(String requiredRole) {
            this.requiredRole = requiredRole;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public void setReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
        }

        public List<String> getRedactedPayloadFields() {
            return redactedPayloadFields;
        }

        public void setRedactedPayloadFields(List<String> redactedPayloadFields) {
            this.redactedPayloadFields = redactedPayloadFields == null ? new ArrayList<>() : redactedPayloadFields;
        }

        public enum AuthMode {
            BASIC,
            SPRING_SECURITY
        }
    }
}
