package com.jobq.config;

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
        private long pollIntervalInSeconds = 15;
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

        public long getPollIntervalInSeconds() {
            return pollIntervalInSeconds;
        }

        public void setPollIntervalInSeconds(long pollIntervalInSeconds) {
            this.pollIntervalInSeconds = pollIntervalInSeconds;
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
        private boolean enabled = true;
        private String path = "/jobq/dashboard";
        private String username = "";
        private String password = "";

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
    }
}
