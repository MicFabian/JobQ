package com.jobq.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.jobq.Job;
import com.jobq.JobLifecycle;
import com.jobq.JobRepository;
import com.jobq.JobRuntime;
import com.jobq.JobTypeNames;
import com.jobq.JobWorker;
import com.jobq.dashboard.JobDashboardEventBus;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

@Component
@ConditionalOnProperty(
        prefix = "jobq.background-job-server",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class JobPoller {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);
    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");
    private static final Pattern SAFE_CHANNEL = Pattern.compile("[A-Za-z0-9_]+");
    private static final long NOTIFICATION_WARNING_THROTTLE_MS = 30_000L;

    private final JobRepository jobRepository;
    private final List<JobWorker<?>> workers;
    private final ListableBeanFactory beanFactory;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final com.jobq.config.JobQProperties properties;
    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final JobSignalPublisher jobSignalPublisher;
    private final JobOperationsService jobOperationsService;
    private final JobRuntime jobRuntime;
    private final JobDashboardEventBus dashboardEventBus;
    private final int workerCount;
    private final int processingQueueCapacity;
    private final ExecutorService processingExecutor;
    private final ThreadPoolExecutor pollingExecutor;
    private final String jobTableName;
    private final String claimNextJobsSql;
    private final String notifyChannel;
    private final int notifyListenTimeoutMs;
    private final boolean virtualThreadsEnabled;

    private final String nodeId = "node-" + UUID.randomUUID().toString();
    private final OffsetDateTime nodeStartedAt = OffsetDateTime.now();
    private final CountDownLatch notificationListenerReady = new CountDownLatch(1);
    private final AtomicBoolean notificationListenerRunning = new AtomicBoolean(false);
    private final AtomicBoolean notificationListenerConnected = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicLong lastNotificationWarningAt = new AtomicLong(0L);
    private final AtomicInteger queuedOrActiveProcessingCount = new AtomicInteger();
    private final AtomicInteger activeProcessingCount = new AtomicInteger();
    private final Map<String, AtomicInteger> inFlightByType = new ConcurrentHashMap<>();
    private final Map<String, Long> lastDispatchAtByType = new ConcurrentHashMap<>();
    private volatile Map<String, JobOperationsService.QueueRuntimeState> queueRuntimeStates = Map.of();
    private Map<String, RegisteredJob> jobMap = Map.of();
    private Map<String, AtomicBoolean> pollInProgress = Map.of();
    private Thread notificationListenerThread;
    private Runnable localSignalSubscription = () -> {};

    public JobPoller(
            JobRepository jobRepository,
            List<JobWorker<?>> workers,
            ListableBeanFactory beanFactory,
            ObjectMapper objectMapper,
            TransactionTemplate transactionTemplate,
            com.jobq.config.JobQProperties properties,
            DataSource dataSource,
            JdbcTemplate jdbcTemplate,
            JobSignalPublisher jobSignalPublisher,
            JobOperationsService jobOperationsService,
            JobRuntime jobRuntime,
            ObjectProvider<JobDashboardEventBus> dashboardEventBusProvider) {
        this.jobRepository = jobRepository;
        this.workers = workers;
        this.beanFactory = beanFactory;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
        this.jobSignalPublisher = jobSignalPublisher;
        this.jobOperationsService = jobOperationsService;
        this.jobRuntime = jobRuntime;
        this.dashboardEventBus = dashboardEventBusProvider.getIfAvailable();
        this.workerCount = Math.max(1, properties.getBackgroundJobServer().getWorkerCount());
        this.virtualThreadsEnabled = properties.getBackgroundJobServer().isVirtualThreadsEnabled();
        this.jobTableName = resolveJobTableName(properties.getDatabase().getTablePrefix());
        this.claimNextJobsSql = buildClaimNextJobsSql(jobTableName);
        this.notifyChannel =
                normalizeNotifyChannel(properties.getBackgroundJobServer().getNotifyChannel());
        this.notifyListenTimeoutMs =
                Math.max(100, properties.getBackgroundJobServer().getNotifyListenTimeoutMs());

        this.processingQueueCapacity = virtualThreadsEnabled ? workerCount : Math.max(32, workerCount * 8);
        this.processingExecutor = createProcessingExecutor();

        int pollThreads = Math.min(4, Math.max(1, workerCount / 2));
        int pollingQueueCapacity = Math.max(32, workerCount * 4);
        ThreadFactory pollingThreadFactory = createPlatformThreadFactory("jobq-polling-");
        this.pollingExecutor = new ThreadPoolExecutor(
                pollThreads,
                pollThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(pollingQueueCapacity),
                pollingThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    @PostConstruct
    public void init() {
        Map<String, RegisteredJob> registrations = new LinkedHashMap<>();

        for (JobWorker<?> worker : workers) {
            registerWorkerBasedJob(registrations, worker);
        }

        Map<String, Object> annotationBeans = beanFactory.getBeansWithAnnotation(com.jobq.annotation.Job.class);
        for (Object bean : annotationBeans.values()) {
            if (bean instanceof JobWorker<?>) {
                continue;
            }
            registerAnnotationDrivenJob(registrations, bean);
        }

        this.jobMap = Map.copyOf(registrations);

        Map<String, AtomicBoolean> pollState = new HashMap<>();
        for (String jobType : jobMap.keySet()) {
            pollState.put(jobType, new AtomicBoolean(false));
        }
        this.pollInProgress = pollState;
        this.queueRuntimeStates = jobOperationsService.loadQueueRuntimeStates();

        log.info("Job Poller initialized on {} with {} registered jobs: {}", nodeId, jobMap.size(), jobMap.keySet());
        this.localSignalSubscription = jobSignalPublisher.registerLocalListener(this::handleNotificationPayload);
        startNotificationListenerIfConfigured();
        publishNodeHeartbeat();
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.poll-interval-in-seconds:15}000")
    public void poll() {
        if (shuttingDown.get() || jobMap.isEmpty()) {
            return;
        }
        refreshQueueRuntimeStates();
        for (String jobType : jobMap.keySet()) {
            dispatchPoll(jobType);
        }
    }

    private void pollForType(String jobType) {
        if (shuttingDown.get()) {
            return;
        }
        RegisteredJob registration = jobMap.get(jobType);
        if (registration == null) {
            return;
        }

        JobOperationsService.QueueRuntimeState queueState =
                queueRuntimeStates.getOrDefault(jobType, JobOperationsService.QueueRuntimeState.defaults(jobType));
        if (queueState.paused() || isDispatchCoolingDown(jobType, queueState)) {
            return;
        }

        int remainingSlots = Math.min(availableProcessingSlots(), availableProcessingSlotsForType(jobType, queueState));
        while (remainingSlots > 0) {
            int batchSize = Math.min(workerCount, remainingSlots);
            batchSize = Math.min(batchSize, rateLimitRemaining(jobType, queueState));
            if (batchSize <= 0) {
                return;
            }
            final int claimBatchSize = batchSize;
            List<Job> jobs =
                    transactionTemplate.execute(status -> claimNextJobs(jobType, registration, claimBatchSize));

            if (jobs == null || jobs.isEmpty()) {
                return;
            }

            rememberDispatch(jobType);
            for (int index = 0; index < jobs.size(); index++) {
                Job job = jobs.get(index);
                if (!reserveProcessingSlot()) {
                    releaseClaimedJobs(jobs, index);
                    return;
                }
                AtomicInteger inFlightCounter = inFlightByType.computeIfAbsent(jobType, ignored -> new AtomicInteger());
                int inFlightAfterIncrement = inFlightCounter.incrementAndGet();
                try {
                    processingExecutor.execute(() -> {
                        activeProcessingCount.incrementAndGet();
                        try {
                            processJob(job, registration);
                        } finally {
                            activeProcessingCount.decrementAndGet();
                            releaseProcessingSlot();
                            inFlightCounter.decrementAndGet();
                        }
                    });
                } catch (RejectedExecutionException saturatedProcessingQueue) {
                    releaseProcessingSlot();
                    inFlightCounter.decrementAndGet();
                    releaseClaimedJob(job);
                    releaseClaimedJobs(jobs, index + 1);
                    log.debug(
                            "Skipping job {} dispatch for type {} because processing queue is saturated",
                            job.getId(),
                            jobType);
                    return;
                }
                if (queueState.maxConcurrency() != null && inFlightAfterIncrement >= queueState.maxConcurrency()) {
                    break;
                }
            }

            remainingSlots = Math.min(availableProcessingSlots(), availableProcessingSlotsForType(jobType, queueState));
            if (jobs.size() < batchSize) {
                return;
            }
        }
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.execution-timeout-check-interval-in-seconds:30}000")
    public void reclaimTimedOutJobs() {
        if (shuttingDown.get() || jobMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, RegisteredJob> entry : jobMap.entrySet()) {
            JobOperationsService.QueueRuntimeState queueState = queueRuntimeStates.getOrDefault(
                    entry.getKey(), JobOperationsService.QueueRuntimeState.defaults(entry.getKey()));
            if (queueState.paused()) {
                continue;
            }
            long maxExecutionMs = maxExecutionMs(entry.getValue());
            if (maxExecutionMs <= 0) {
                continue;
            }

            OffsetDateTime lockedBefore = OffsetDateTime.now().minusNanos(maxExecutionMs * 1_000_000);
            PageRequest pageRequest = PageRequest.of(0, Math.max(1, workerCount));
            Slice<Job> timedOutJobs =
                    jobRepository.findTimedOutProcessingJobs(entry.getKey(), lockedBefore, pageRequest);
            for (Job timedOutJob : timedOutJobs.getContent()) {
                handleTimeout(timedOutJob, entry.getValue(), maxExecutionMs);
            }
        }
    }

    @Scheduled(fixedDelayString = "${jobq.background-job-server.node-heartbeat-interval-in-seconds:10}000")
    public void publishNodeHeartbeat() {
        if (jobMap.isEmpty()) {
            return;
        }
        jobOperationsService.heartbeatNode(
                nodeId,
                nodeStartedAt,
                OffsetDateTime.now(),
                workerCount,
                activeProcessingCount.get(),
                jobMap.keySet(),
                properties.getBackgroundJobServer().isNotifyEnabled());
    }

    private boolean shouldReleaseGroupedJobsWhenFirstDue(RegisteredJob registration) {
        com.jobq.annotation.Job annotation = registration.annotation();
        return annotation == null
                || annotation.groupDelayPolicy()
                        == com.jobq.annotation.Job.GroupDelayPolicy.KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE;
    }

    private void dispatchPoll(String jobType) {
        if (shuttingDown.get()) {
            return;
        }
        JobOperationsService.QueueRuntimeState queueState =
                queueRuntimeStates.getOrDefault(jobType, JobOperationsService.QueueRuntimeState.defaults(jobType));
        if (queueState.paused()) {
            return;
        }
        AtomicBoolean inProgress = pollInProgress.get(jobType);
        if (inProgress == null || !inProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            pollingExecutor.execute(() -> {
                try {
                    pollForType(jobType);
                } finally {
                    inProgress.set(false);
                }
            });
        } catch (RejectedExecutionException saturatedPollingQueue) {
            inProgress.set(false);
            log.debug("Skipping poll dispatch for type {} because polling queue is saturated", jobType);
        }
    }

    private List<Job> claimNextJobs(String jobType, RegisteredJob registration, int batchSize) {
        if (shuttingDown.get()) {
            return List.of();
        }
        OffsetDateTime lockTime = OffsetDateTime.now();
        if (shouldReleaseGroupedJobsWhenFirstDue(registration)) {
            List<String> dueGroupIds = jobRepository.findDueActiveGroupIds(jobType, PageRequest.of(0, batchSize));
            if (!dueGroupIds.isEmpty()) {
                jobRepository.releaseGroupedPendingJobs(jobType, dueGroupIds, lockTime);
            }
        }

        return jdbcTemplate.query(
                claimNextJobsSql,
                preparedStatement -> {
                    preparedStatement.setString(1, jobType);
                    preparedStatement.setObject(2, lockTime);
                    preparedStatement.setInt(3, batchSize);
                    preparedStatement.setObject(4, lockTime);
                    preparedStatement.setObject(5, lockTime);
                    preparedStatement.setString(6, nodeId);
                    preparedStatement.setObject(7, lockTime);
                },
                (resultSet, rowNum) -> mapClaimedJob(resultSet));
    }

    private Job mapClaimedJob(ResultSet resultSet) throws SQLException {
        Job job = new Job();
        job.setId(resultSet.getObject("id", UUID.class));
        job.setType(resultSet.getString("type"));
        String payload = resultSet.getString("payload");
        if (payload != null) {
            try {
                job.setPayload(objectMapper.readTree(payload));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to deserialize claimed payload for job " + job.getId(), e);
            }
        }
        job.setCreatedAt(resultSet.getObject("created_at", OffsetDateTime.class));
        job.setUpdatedAt(resultSet.getObject("updated_at", OffsetDateTime.class));
        job.setLockedAt(resultSet.getObject("locked_at", OffsetDateTime.class));
        job.setLockedBy(resultSet.getString("locked_by"));
        job.setProcessingStartedAt(resultSet.getObject("processing_started_at", OffsetDateTime.class));
        job.setFinishedAt(resultSet.getObject("finished_at", OffsetDateTime.class));
        job.setFailedAt(resultSet.getObject("failed_at", OffsetDateTime.class));
        job.setErrorMessage(resultSet.getString("error_message"));
        job.setRetryCount(resultSet.getInt("retry_count"));
        job.setMaxRetries(resultSet.getInt("max_retries"));
        job.setPriority(resultSet.getInt("priority"));
        job.setRunAt(resultSet.getObject("run_at", OffsetDateTime.class));
        job.setGroupId(resultSet.getString("group_id"));
        job.setReplaceKey(resultSet.getString("replace_key"));
        job.setCron(resultSet.getString("cron"));
        return job;
    }

    private void releaseClaimedJobs(List<Job> claimedJobs, int startIndex) {
        for (int index = startIndex; index < claimedJobs.size(); index++) {
            releaseClaimedJob(claimedJobs.get(index));
        }
    }

    private void releaseClaimedJob(Job job) {
        OffsetDateTime releasedAt = OffsetDateTime.now();
        Integer updated = transactionTemplate.execute(
                status -> jobRepository.releaseClaimedJob(job.getId(), job.getLockedAt(), nodeId, releasedAt));
        if (toAffectedRows(updated) == 0) {
            log.debug("Claim release skipped for job {} because it was already transitioned", job.getId());
        }
    }

    private void refreshQueueRuntimeStates() {
        this.queueRuntimeStates = jobOperationsService.loadQueueRuntimeStates();
    }

    private int availableProcessingSlotsForType(
            String jobType, JobOperationsService.QueueRuntimeState queueRuntimeState) {
        Integer maxConcurrency = queueRuntimeState.maxConcurrency();
        if (maxConcurrency == null) {
            return workerCount;
        }
        int inFlight = inFlightByType
                .computeIfAbsent(jobType, ignored -> new AtomicInteger())
                .get();
        return Math.max(0, maxConcurrency - inFlight);
    }

    private int rateLimitRemaining(String jobType, JobOperationsService.QueueRuntimeState queueRuntimeState) {
        Integer rateLimitPerMinute = queueRuntimeState.rateLimitPerMinute();
        if (rateLimitPerMinute == null) {
            return Integer.MAX_VALUE;
        }
        long startedInLastMinute =
                jobRepository.countStartedSince(jobType, OffsetDateTime.now().minusMinutes(1));
        return Math.max(0, rateLimitPerMinute - (int) startedInLastMinute);
    }

    private boolean isDispatchCoolingDown(String jobType, JobOperationsService.QueueRuntimeState queueRuntimeState) {
        Integer dispatchCooldownMs = queueRuntimeState.dispatchCooldownMs();
        if (dispatchCooldownMs == null || dispatchCooldownMs <= 0) {
            return false;
        }
        long lastDispatchAt = lastDispatchAtByType.getOrDefault(jobType, 0L);
        return System.currentTimeMillis() - lastDispatchAt < dispatchCooldownMs;
    }

    private void rememberDispatch(String jobType) {
        lastDispatchAtByType.put(jobType, System.currentTimeMillis());
    }

    private int availableProcessingSlots() {
        return Math.max(0, processingQueueCapacity - queuedOrActiveProcessingCount.get());
    }

    private String resolveJobTableName(String tablePrefix) {
        String prefix = tablePrefix == null ? "" : tablePrefix.trim();
        String tableName = prefix + "jobq_jobs";
        if (!SAFE_TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Unsupported job table name: " + tableName);
        }
        return tableName;
    }

    private String buildClaimNextJobsSql(String tableName) {
        return """
                WITH candidates AS (
                    SELECT id
                    FROM %1$s
                    WHERE type = ?
                      AND processing_started_at IS NULL
                      AND finished_at IS NULL
                      AND failed_at IS NULL
                      AND cancelled_at IS NULL
                      AND run_at <= ?
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?
                )
                UPDATE %1$s j
                SET processing_started_at = ?,
                    finished_at = NULL,
                    failed_at = NULL,
                    locked_at = ?,
                    locked_by = ?,
                    updated_at = ?
                FROM candidates c
                WHERE j.id = c.id
                RETURNING
                    j.id,
                    j.type,
                    j.payload,
                    j.created_at,
                    j.updated_at,
                    j.locked_at,
                    j.locked_by,
                    j.processing_started_at,
                    j.finished_at,
                    j.failed_at,
                    j.error_message,
                    j.retry_count,
                    j.max_retries,
                    j.priority,
                    j.run_at,
                    j.group_id,
                    j.replace_key,
                    j.cron
                """
                .formatted(tableName);
    }

    private String normalizeNotifyChannel(String configuredChannel) {
        String candidate = configuredChannel == null ? "" : configuredChannel.trim();
        String normalized = candidate.isEmpty() ? "jobq_jobs_available" : candidate;
        if (!SAFE_CHANNEL.matcher(normalized).matches()) {
            throw new IllegalArgumentException("Unsupported PostgreSQL notify channel name: " + normalized);
        }
        return normalized;
    }

    private void startNotificationListenerIfConfigured() {
        if (!properties.getBackgroundJobServer().isNotifyEnabled()) {
            return;
        }
        if (!notificationListenerRunning.compareAndSet(false, true)) {
            return;
        }
        notificationListenerThread =
                createManagedThread("jobq-pg-listener-" + nodeId, this::runNotificationListener, true);
        notificationListenerThread.start();
        try {
            if (!notificationListenerReady.await(5, TimeUnit.SECONDS)) {
                log.warn("JobQ PostgreSQL listener did not become ready within startup timeout");
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private void runNotificationListener() {
        while (notificationListenerRunning.get()) {
            try (java.sql.Connection connection = dataSource.getConnection();
                    java.sql.Statement statement = connection.createStatement()) {
                connection.setAutoCommit(true);
                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                statement.execute("LISTEN " + notifyChannel);
                notificationListenerReady.countDown();
                boolean recovered = !notificationListenerConnected.getAndSet(true);
                if (recovered) {
                    log.info("JobQ PostgreSQL listener active on channel {}", notifyChannel);
                }

                while (notificationListenerRunning.get()) {
                    PGNotification[] notifications = pgConnection.getNotifications(notifyListenTimeoutMs);
                    if (notifications == null || notifications.length == 0) {
                        continue;
                    }
                    for (PGNotification notification : notifications) {
                        handleNotificationPayload(notification.getParameter());
                    }
                }
            } catch (Exception e) {
                notificationListenerReady.countDown();
                boolean wasConnected = notificationListenerConnected.getAndSet(false);
                if (shuttingDown.get()
                        || !notificationListenerRunning.get()
                        || Thread.currentThread().isInterrupted()) {
                    log.debug("JobQ PostgreSQL listener stopped during shutdown");
                    break;
                }
                if (notificationListenerRunning.get()) {
                    logNotificationListenerFailure(e, wasConnected);
                    sleepQuietly(1_000);
                }
            }
        }
    }

    private void logNotificationListenerFailure(Exception exception, boolean wasConnected) {
        String failureSummary = summarizeException(exception);
        if (shouldWarnNotificationListenerFailure(wasConnected, System.currentTimeMillis())) {
            log.warn(
                    "JobQ PostgreSQL listener unavailable on channel {}. Retrying in 1s. Cause: {}",
                    notifyChannel,
                    failureSummary);
            return;
        }
        log.debug("JobQ PostgreSQL listener retry suppressed on channel {}. Cause: {}", notifyChannel, failureSummary);
    }

    boolean shouldWarnNotificationListenerFailure(boolean wasConnected, long nowMillis) {
        if (wasConnected) {
            lastNotificationWarningAt.set(nowMillis);
            return true;
        }

        long lastWarning = lastNotificationWarningAt.get();
        return nowMillis - lastWarning >= NOTIFICATION_WARNING_THROTTLE_MS
                && lastNotificationWarningAt.compareAndSet(lastWarning, nowMillis);
    }

    String summarizeException(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        if (message == null || message.isBlank()) {
            return current.getClass().getSimpleName();
        }
        return current.getClass().getSimpleName() + ": " + message;
    }

    private void handleNotificationPayload(String payload) {
        if (payload == null || payload.isBlank() || "*".equals(payload)) {
            poll();
            return;
        }
        if (jobMap.containsKey(payload)) {
            dispatchPoll(payload);
            return;
        }
        poll();
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private ExecutorService createProcessingExecutor() {
        if (virtualThreadsEnabled) {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        return new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(processingQueueCapacity),
                createPlatformThreadFactory("jobq-processing-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private boolean reserveProcessingSlot() {
        while (true) {
            int current = queuedOrActiveProcessingCount.get();
            if (current >= processingQueueCapacity) {
                return false;
            }
            if (queuedOrActiveProcessingCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private void releaseProcessingSlot() {
        queuedOrActiveProcessingCount.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    private ThreadFactory createPlatformThreadFactory(String threadNamePrefix) {
        AtomicInteger threadCounter = new AtomicInteger();
        return runnable -> createManagedThread(threadNamePrefix + threadCounter.incrementAndGet(), runnable, false);
    }

    private Thread createManagedThread(String threadName, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(daemon);
        return thread;
    }

    private long maxExecutionMs(RegisteredJob registration) {
        com.jobq.annotation.Job annotation = registration.annotation();
        if (annotation == null) {
            return 0L;
        }
        return Math.max(0L, annotation.maxExecutionMs());
    }

    private void processJob(Job job, RegisteredJob registration) {
        if (shuttingDown.get()) {
            return;
        }
        log.debug("Locked job {} of type {} for processing", job.getId(), job.getType());

        Object payload = null;
        try (JobRuntime.JobExecutionHandle ignored =
                jobRuntime.open(job.getId(), job.getType(), job.getLockedAt(), nodeId)) {
            try {
                payload = registration.payloadDeserializer().deserialize(job.getPayload());
                registration.invoker().invoke(job.getId(), payload);
                if (shuttingDown.get()) {
                    return;
                }
                if (markCompleted(job, registration)) {
                    invokeOnSuccessSafely(registration, job.getId(), payload);
                    log.debug("Successfully completed job {} of type {}", job.getId(), job.getType());
                } else {
                    log.debug(
                            "Skipped completion callbacks for job {} because state no longer allows completion",
                            job.getId());
                }
            } catch (Exception e) {
                invokeOnErrorSafely(registration, job.getId(), payload, e);
                if (isExpectedException(registration, e)) {
                    log.debug(
                            "Job {} of type {} threw expected exception {}. Marking as COMPLETED.",
                            job.getId(),
                            job.getType(),
                            e.getClass().getName());
                    if (markCompleted(job, registration)) {
                        invokeOnSuccessSafely(registration, job.getId(), payload);
                    }
                    return;
                }

                if (shuttingDown.get()) {
                    return;
                }
                log.error("Failed to process job {} of type {}", job.getId(), job.getType(), e);
                handleFailure(job, e, registration);
            } finally {
                invokeAfterSafely(registration, job.getId(), payload);
            }
        }
    }

    private void invokeOnErrorSafely(RegisteredJob registration, UUID jobId, Object payload, Exception error) {
        try {
            registration.errorHandler().onError(jobId, payload, error);
        } catch (Exception onErrorFailure) {
            log.error("onError callback failed for job {} of type {}", jobId, registration.type(), onErrorFailure);
        }
    }

    private void invokeOnSuccessSafely(RegisteredJob registration, UUID jobId, Object payload) {
        try {
            registration.successHandler().onSuccess(jobId, payload);
        } catch (Exception onSuccessFailure) {
            log.error("onSuccess callback failed for job {} of type {}", jobId, registration.type(), onSuccessFailure);
        }
    }

    private void invokeAfterSafely(RegisteredJob registration, UUID jobId, Object payload) {
        try {
            registration.afterHandler().after(jobId, payload);
        } catch (Exception afterFailure) {
            log.error("after callback failed for job {} of type {}", jobId, registration.type(), afterFailure);
        }
    }

    private void registerWorkerBasedJob(Map<String, RegisteredJob> registrations, JobWorker<?> worker) {
        String jobType = worker.getJobType();
        Class<?> payloadClass = worker.getPayloadClass();
        com.jobq.annotation.Job annotation = findJobAnnotationOnBean(worker);
        PayloadDeserializer payloadDeserializer = payloadDeserializerFor(payloadClass);

        JobInvoker invoker = (jobId, payload) -> invokeWorker(worker, jobId, payload);
        JobErrorHandler errorHandler =
                (jobId, payload, exception) -> invokeWorkerOnError(worker, jobId, payload, exception);
        JobSuccessHandler successHandler = (jobId, payload) -> invokeWorkerOnSuccess(worker, jobId, payload);
        JobAfterHandler afterHandler = (jobId, payload) -> invokeWorkerAfter(worker, jobId, payload);
        registerJob(
                registrations,
                jobType,
                payloadDeserializer,
                annotation,
                invoker,
                errorHandler,
                successHandler,
                afterHandler,
                "JobWorker bean " + ClassUtils.getUserClass(worker).getName());
    }

    private void registerAnnotationDrivenJob(Map<String, RegisteredJob> registrations, Object bean) {
        com.jobq.annotation.Job annotation = findJobAnnotationOnBean(bean);
        if (annotation == null) {
            return;
        }

        Class<?> beanClass = ClassUtils.getUserClass(bean);
        String jobType = resolveConfiguredTypeOrClassName(annotation.value(), beanClass);

        if (bean instanceof JobLifecycle<?> lifecycle) {
            Class<?> payloadClass = resolveLifecyclePayloadClass(bean, annotation);
            PayloadDeserializer payloadDeserializer = payloadDeserializerFor(payloadClass);
            JobInvoker invoker = (jobId, payload) -> invokeLifecycleProcess(lifecycle, jobId, payload);
            JobErrorHandler errorHandler =
                    (jobId, payload, exception) -> invokeLifecycleOnError(lifecycle, jobId, payload, exception);
            JobSuccessHandler successHandler = (jobId, payload) -> invokeLifecycleOnSuccess(lifecycle, jobId, payload);
            JobAfterHandler afterHandler = (jobId, payload) -> invokeLifecycleAfter(lifecycle, jobId, payload);

            registerJob(
                    registrations,
                    jobType,
                    payloadDeserializer,
                    annotation,
                    invoker,
                    errorHandler,
                    successHandler,
                    afterHandler,
                    "@Job bean " + ClassUtils.getUserClass(bean).getName());
            return;
        }

        Method processMethod = resolveProcessMethod(bean, annotation);
        Class<?> payloadClass = resolvePayloadClass(annotation, processMethod);
        PayloadDeserializer payloadDeserializer = payloadDeserializerFor(payloadClass);
        Method onErrorMethod = resolveOnErrorMethod(bean, payloadClass);
        Method onSuccessMethod = resolveOnSuccessMethod(bean, payloadClass);
        Method afterMethod = resolveAfterMethod(bean, payloadClass);
        JobInvoker invoker = createProcessInvoker(bean, processMethod);
        JobErrorHandler errorHandler = createOnErrorHandler(bean, onErrorMethod);
        JobSuccessHandler successHandler = createOnSuccessHandler(bean, onSuccessMethod);
        JobAfterHandler afterHandler = createAfterHandler(bean, afterMethod);

        registerJob(
                registrations,
                jobType,
                payloadDeserializer,
                annotation,
                invoker,
                errorHandler,
                successHandler,
                afterHandler,
                "@Job bean " + ClassUtils.getUserClass(bean).getName());
    }

    private Class<?> resolveLifecyclePayloadClass(Object bean, com.jobq.annotation.Job annotation) {
        if (annotation.payload() != Void.class) {
            return annotation.payload();
        }

        Class<?> targetClass = ClassUtils.getUserClass(bean);
        Class<?> resolvedFromGeneric = ResolvableType.forClass(targetClass)
                .as(JobLifecycle.class)
                .getGeneric(0)
                .resolve();
        if (resolvedFromGeneric != null) {
            return resolvedFromGeneric;
        }

        Method processMethod = resolveProcessMethod(bean, annotation);
        return resolvePayloadClass(annotation, processMethod);
    }

    private String resolveConfiguredTypeOrClassName(String configuredType, Class<?> ownerClass) {
        return JobTypeNames.configuredOrDefault(configuredType, ownerClass);
    }

    private void registerJob(
            Map<String, RegisteredJob> registrations,
            String jobType,
            PayloadDeserializer payloadDeserializer,
            com.jobq.annotation.Job annotation,
            JobInvoker invoker,
            JobErrorHandler errorHandler,
            JobSuccessHandler successHandler,
            JobAfterHandler afterHandler,
            String source) {
        String recurringCron = null;
        CronExpression recurringCronExpression = null;
        if (annotation != null
                && annotation.cron() != null
                && !annotation.cron().isBlank()) {
            recurringCron = annotation.cron().trim();
            try {
                recurringCronExpression = CronExpression.parse(recurringCron);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Invalid cron expression '" + recurringCron + "' for job type '" + jobType + "'", e);
            }
        }
        RegisteredJob existing = registrations.putIfAbsent(
                jobType,
                new RegisteredJob(
                        jobType,
                        payloadDeserializer,
                        annotation,
                        invoker,
                        errorHandler,
                        successHandler,
                        afterHandler,
                        recurringCron,
                        recurringCronExpression));
        if (existing != null) {
            throw new IllegalStateException("Duplicate job type '" + jobType + "' detected while registering " + source
                    + ". Each job type must be unique.");
        }
    }

    private Method resolveProcessMethod(Object bean, com.jobq.annotation.Job annotation) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> processMethods = Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals("process"))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();

        if (processMethods.isEmpty()) {
            throw new IllegalStateException(
                    "@Job bean " + targetClass.getName() + " must declare a non-static process(...) method.");
        }

        Class<?> configuredPayload = annotation.payload();

        if (configuredPayload != Void.class) {
            Method exactTwoArgs = findUniqueMethod(
                    processMethods,
                    method -> hasSignature(method, UUID.class, configuredPayload),
                    targetClass,
                    "(UUID, " + configuredPayload.getSimpleName() + ")");
            if (exactTwoArgs != null) {
                return exactTwoArgs;
            }

            Method assignableTwoArgs = findUniqueMethod(
                    processMethods,
                    method -> method.getParameterCount() == 2
                            && UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[1].isAssignableFrom(configuredPayload),
                    targetClass,
                    "(UUID, <assignable " + configuredPayload.getSimpleName() + ">)");
            if (assignableTwoArgs != null) {
                return assignableTwoArgs;
            }

            Method exactOneArg = findUniqueMethod(
                    processMethods,
                    method -> hasSignature(method, configuredPayload),
                    targetClass,
                    "(" + configuredPayload.getSimpleName() + ")");
            if (exactOneArg != null) {
                return exactOneArg;
            }

            Method assignableOneArg = findUniqueMethod(
                    processMethods,
                    method -> method.getParameterCount() == 1
                            && !UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[0].isAssignableFrom(configuredPayload),
                    targetClass,
                    "(<assignable " + configuredPayload.getSimpleName() + ">)");
            if (assignableOneArg != null) {
                return assignableOneArg;
            }

            throw new IllegalStateException("@Job bean " + targetClass.getName() + " declares payload "
                    + configuredPayload.getName() + " but no matching process(...) method was found.");
        }

        Method inferredTwoArgs = findUniqueMethod(
                processMethods,
                method -> method.getParameterCount() == 2 && UUID.class.isAssignableFrom(method.getParameterTypes()[0]),
                targetClass,
                "(UUID, Payload)");
        if (inferredTwoArgs != null) {
            return inferredTwoArgs;
        }

        Method inferredOnePayloadArg = findUniqueMethod(
                processMethods,
                method ->
                        method.getParameterCount() == 1 && !UUID.class.isAssignableFrom(method.getParameterTypes()[0]),
                targetClass,
                "(Payload)");
        if (inferredOnePayloadArg != null) {
            return inferredOnePayloadArg;
        }

        Method uuidOnly =
                findUniqueMethod(processMethods, method -> hasSignature(method, UUID.class), targetClass, "(UUID)");
        if (uuidOnly != null) {
            return uuidOnly;
        }

        Method noArgs = findUniqueMethod(processMethods, method -> method.getParameterCount() == 0, targetClass, "()");
        if (noArgs != null) {
            return noArgs;
        }

        throw new IllegalStateException(
                "@Job bean " + targetClass.getName()
                        + " has no supported process(...) signature. Supported: process(), process(UUID), process(Payload), process(UUID, Payload).");
    }

    private Method findUniqueMethod(
            List<Method> methods,
            java.util.function.Predicate<Method> matcher,
            Class<?> targetClass,
            String signatureDescription) {
        List<Method> matches = methods.stream().filter(matcher).toList();
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Ambiguous method overloads on " + targetClass.getName() + " for signature "
                    + signatureDescription + ". Keep exactly one matching method.");
        }
        Method selected = matches.get(0);
        ReflectionUtils.makeAccessible(selected);
        return selected;
    }

    private boolean hasSignature(Method method, Class<?>... parameterTypes) {
        return Arrays.equals(method.getParameterTypes(), parameterTypes);
    }

    private Class<?> resolvePayloadClass(com.jobq.annotation.Job annotation, Method processMethod) {
        if (annotation.payload() != Void.class) {
            return annotation.payload();
        }
        Class<?>[] parameterTypes = processMethod.getParameterTypes();
        if (parameterTypes.length == 2) {
            return parameterTypes[1];
        }
        if (parameterTypes.length == 1 && !UUID.class.isAssignableFrom(parameterTypes[0])) {
            return parameterTypes[0];
        }
        return Void.class;
    }

    private PayloadDeserializer payloadDeserializerFor(Class<?> payloadClass) {
        if (payloadClass == Void.class) {
            return rawPayload -> null;
        }

        ObjectReader reader = objectMapper.readerFor(payloadClass);
        return rawPayload -> {
            if (rawPayload == null) {
                return null;
            }
            return reader.readValue(rawPayload);
        };
    }

    private Method resolveOnErrorMethod(Object bean, Class<?> payloadClass) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> onErrorMethods = Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals("onError"))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();

        if (onErrorMethods.isEmpty()) {
            return null;
        }

        if (payloadClass != Void.class) {
            Method fullContext = findUniqueMethod(
                    onErrorMethods,
                    method -> hasErrorSignature(method, UUID.class, payloadClass),
                    targetClass,
                    "onError(UUID, Payload, Exception)");
            if (fullContext != null) {
                return fullContext;
            }

            Method payloadAndError = findUniqueMethod(
                    onErrorMethods,
                    method -> hasErrorSignature(method, payloadClass),
                    targetClass,
                    "onError(Payload, Exception)");
            if (payloadAndError != null) {
                return payloadAndError;
            }
        }

        Method idAndError = findUniqueMethod(
                onErrorMethods,
                method -> hasErrorSignature(method, UUID.class),
                targetClass,
                "onError(UUID, Exception)");
        if (idAndError != null) {
            return idAndError;
        }

        Method onlyError =
                findUniqueMethod(onErrorMethods, this::hasErrorOnlySignature, targetClass, "onError(Exception)");
        if (onlyError != null) {
            return onlyError;
        }

        throw new IllegalStateException(
                "@Job bean " + targetClass.getName()
                        + " has unsupported onError(...) signatures. Supported: onError(Exception), onError(UUID, Exception), onError(Payload, Exception), onError(UUID, Payload, Exception).");
    }

    private Method resolveOnSuccessMethod(Object bean, Class<?> payloadClass) {
        return resolveCompletionCallbackMethod(bean, payloadClass, "onSuccess");
    }

    private Method resolveAfterMethod(Object bean, Class<?> payloadClass) {
        return resolveCompletionCallbackMethod(bean, payloadClass, "after");
    }

    private Method resolveCompletionCallbackMethod(Object bean, Class<?> payloadClass, String callbackName) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        List<Method> callbackMethods = findLifecycleCallbackMethods(targetClass, callbackName);
        if (callbackMethods.isEmpty()) {
            return null;
        }

        if (payloadClass != Void.class) {
            Method fullContext = findUniqueMethod(
                    callbackMethods,
                    method -> hasSignature(method, UUID.class, payloadClass),
                    targetClass,
                    callbackName + "(UUID, Payload)");
            if (fullContext != null) {
                return fullContext;
            }

            Method payloadOnly = findUniqueMethod(
                    callbackMethods,
                    method -> hasSignature(method, payloadClass),
                    targetClass,
                    callbackName + "(Payload)");
            if (payloadOnly != null) {
                return payloadOnly;
            }

            Method assignableTwoArgs = findUniqueMethod(
                    callbackMethods,
                    method -> method.getParameterCount() == 2
                            && UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[1].isAssignableFrom(payloadClass),
                    targetClass,
                    callbackName + "(UUID, <assignable Payload>)");
            if (assignableTwoArgs != null) {
                return assignableTwoArgs;
            }

            Method assignablePayloadOnly = findUniqueMethod(
                    callbackMethods,
                    method -> method.getParameterCount() == 1
                            && !UUID.class.isAssignableFrom(method.getParameterTypes()[0])
                            && method.getParameterTypes()[0].isAssignableFrom(payloadClass),
                    targetClass,
                    callbackName + "(<assignable Payload>)");
            if (assignablePayloadOnly != null) {
                return assignablePayloadOnly;
            }
        }

        Method idOnly = findUniqueMethod(
                callbackMethods, method -> hasSignature(method, UUID.class), targetClass, callbackName + "(UUID)");
        if (idOnly != null) {
            return idOnly;
        }

        Method noArgs = findUniqueMethod(
                callbackMethods, method -> method.getParameterCount() == 0, targetClass, callbackName + "()");
        if (noArgs != null) {
            return noArgs;
        }

        throw new IllegalStateException("@Job bean " + targetClass.getName()
                + " has unsupported " + callbackName
                + "(...) signatures. Supported: " + callbackName
                + "(), " + callbackName
                + "(UUID), " + callbackName + "(Payload), " + callbackName + "(UUID, Payload).");
    }

    private List<Method> findLifecycleCallbackMethods(Class<?> targetClass, String methodName) {
        return Arrays.stream(ReflectionUtils.getAllDeclaredMethods(targetClass))
                .filter(method -> method.getName().equals(methodName))
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !method.isBridge() && !method.isSynthetic())
                .toList();
    }

    private boolean hasErrorSignature(Method method, Class<?>... leadingParameterTypes) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != leadingParameterTypes.length + 1) {
            return false;
        }
        for (int i = 0; i < leadingParameterTypes.length; i++) {
            if (!parameterTypes[i].equals(leadingParameterTypes[i])) {
                return false;
            }
        }
        return acceptsSupportedOnErrorExceptionType(parameterTypes[parameterTypes.length - 1]);
    }

    private boolean hasErrorOnlySignature(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes.length == 1 && acceptsSupportedOnErrorExceptionType(parameterTypes[0]);
    }

    private boolean acceptsSupportedOnErrorExceptionType(Class<?> parameterType) {
        return parameterType == Exception.class || parameterType == Throwable.class;
    }

    private JobInvoker createProcessInvoker(Object bean, Method processMethod) {
        int parameterCount = processMethod.getParameterCount();
        if (parameterCount == 0) {
            return (jobId, payload) -> invokeReflectively(bean, processMethod);
        }
        if (parameterCount == 1) {
            Class<?> singleParameter = processMethod.getParameterTypes()[0];
            if (UUID.class.isAssignableFrom(singleParameter)) {
                return (jobId, payload) -> invokeReflectively(bean, processMethod, jobId);
            }
            return (jobId, payload) -> invokeReflectively(bean, processMethod, payload);
        }
        return (jobId, payload) -> invokeReflectively(bean, processMethod, jobId, payload);
    }

    private JobErrorHandler createOnErrorHandler(Object bean, Method onErrorMethod) {
        if (onErrorMethod == null) {
            return (jobId, payload, exception) -> {};
        }

        int parameterCount = onErrorMethod.getParameterCount();
        if (parameterCount == 1) {
            return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, exception);
        }
        if (parameterCount == 2) {
            if (UUID.class.isAssignableFrom(onErrorMethod.getParameterTypes()[0])) {
                return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, jobId, exception);
            }
            return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, payload, exception);
        }
        return (jobId, payload, exception) -> invokeReflectively(bean, onErrorMethod, jobId, payload, exception);
    }

    private JobSuccessHandler createOnSuccessHandler(Object bean, Method onSuccessMethod) {
        if (onSuccessMethod == null) {
            return (jobId, payload) -> {};
        }

        int parameterCount = onSuccessMethod.getParameterCount();
        if (parameterCount == 0) {
            return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod);
        }
        if (parameterCount == 1) {
            if (UUID.class.isAssignableFrom(onSuccessMethod.getParameterTypes()[0])) {
                return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, jobId);
            }
            return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, payload);
        }
        return (jobId, payload) -> invokeReflectively(bean, onSuccessMethod, jobId, payload);
    }

    private JobAfterHandler createAfterHandler(Object bean, Method afterMethod) {
        if (afterMethod == null) {
            return (jobId, payload) -> {};
        }

        int parameterCount = afterMethod.getParameterCount();
        if (parameterCount == 0) {
            return (jobId, payload) -> invokeReflectively(bean, afterMethod);
        }
        if (parameterCount == 1) {
            if (UUID.class.isAssignableFrom(afterMethod.getParameterTypes()[0])) {
                return (jobId, payload) -> invokeReflectively(bean, afterMethod, jobId);
            }
            return (jobId, payload) -> invokeReflectively(bean, afterMethod, payload);
        }
        return (jobId, payload) -> invokeReflectively(bean, afterMethod, jobId, payload);
    }

    private void invokeReflectively(Object bean, Method method, Object... args) throws Exception {
        try {
            method.invoke(bean, args);
        } catch (InvocationTargetException invocationTargetException) {
            Throwable target = invocationTargetException.getTargetException();
            if (target instanceof Exception ex) {
                throw ex;
            }
            if (target instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(target);
        }
    }

    private void invokeWorker(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.process(jobId, payload);
    }

    private void invokeWorkerOnError(JobWorker<?> worker, UUID jobId, Object payload, Exception exception)
            throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.onError(jobId, payload, exception);
    }

    private void invokeWorkerOnSuccess(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.onSuccess(jobId, payload);
    }

    private void invokeWorkerAfter(JobWorker<?> worker, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobWorker<Object> castWorker = (JobWorker<Object>) worker;
        castWorker.after(jobId, payload);
    }

    private void invokeLifecycleProcess(JobLifecycle<?> lifecycle, UUID jobId, Object payload) throws Exception {
        @SuppressWarnings("unchecked")
        JobLifecycle<Object> castLifecycle = (JobLifecycle<Object>) lifecycle;
        castLifecycle.process(jobId, payload);
    }

    private void invokeLifecycleOnError(JobLifecycle<?> lifecycle, UUID jobId, Object payload, Exception exception) {
        @SuppressWarnings("unchecked")
        JobLifecycle<Object> castLifecycle = (JobLifecycle<Object>) lifecycle;
        castLifecycle.onError(jobId, payload, exception);
    }

    private void invokeLifecycleOnSuccess(JobLifecycle<?> lifecycle, UUID jobId, Object payload) {
        @SuppressWarnings("unchecked")
        JobLifecycle<Object> castLifecycle = (JobLifecycle<Object>) lifecycle;
        castLifecycle.onSuccess(jobId, payload);
    }

    private void invokeLifecycleAfter(JobLifecycle<?> lifecycle, UUID jobId, Object payload) {
        @SuppressWarnings("unchecked")
        JobLifecycle<Object> castLifecycle = (JobLifecycle<Object>) lifecycle;
        castLifecycle.after(jobId, payload);
    }

    private com.jobq.annotation.Job findJobAnnotationOnBean(Object bean) {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        return AnnotationUtils.findAnnotation(targetClass, com.jobq.annotation.Job.class);
    }

    private boolean markCompleted(Job jobSnapshot, RegisteredJob registration) {
        OffsetDateTime now = OffsetDateTime.now();
        Integer updated = transactionTemplate.execute(
                status -> jobRepository.markCompleted(jobSnapshot.getId(), jobSnapshot.getLockedAt(), now, nodeId));
        if (toAffectedRows(updated) > 0) {
            scheduleNextRecurringExecutionIfNeeded(jobSnapshot, registration);
            publishDashboardRefresh(jobSnapshot.getId());
            return true;
        }

        log.debug("Falling back to entity completion update for job {}", jobSnapshot.getId());
        Boolean fallbackCompleted = transactionTemplate.execute(status -> jobRepository
                .findById(jobSnapshot.getId())
                .map(job -> {
                    if (!isMutableProcessingJob(job, jobSnapshot.getLockedAt())) {
                        log.debug(
                                "Skipping completion fallback for job {} due lifecycle/lock mismatch",
                                jobSnapshot.getId());
                        return false;
                    }
                    if (job.getProcessingStartedAt() == null) {
                        job.setProcessingStartedAt(now);
                    }
                    job.setFinishedAt(now);
                    job.setFailedAt(null);
                    job.setErrorMessage(null);
                    job.setLockedAt(null);
                    job.setLockedBy(null);
                    job.setUpdatedAt(now);
                    job.setProgressPercent(100);
                    jobRepository.save(job);
                    scheduleNextRecurringExecutionIfNeeded(job, registration);
                    publishDashboardRefresh(jobSnapshot.getId());
                    return true;
                })
                .orElse(false));
        return Boolean.TRUE.equals(fallbackCompleted);
    }

    private void scheduleNextRecurringExecutionIfNeeded(Job job, RegisteredJob registration) {
        if (job.getCron() == null || job.getCron().isBlank()) {
            return;
        }
        if (registration.recurringCronExpression() == null || registration.recurringCron() == null) {
            return;
        }

        try {
            OffsetDateTime nextRun = resolveNextRecurringRunAt(job, registration, OffsetDateTime.now());
            if (nextRun == null) {
                return;
            }

            Job nextJob = new Job(
                    UUID.randomUUID(),
                    job.getType(),
                    job.getPayload(),
                    job.getMaxRetries(),
                    job.getPriority(),
                    job.getGroupId(),
                    recurringReplaceKey(registration.recurringCron()));
            nextJob.setCron(registration.recurringCron());
            nextJob.setRunAt(nextRun);
            try {
                jobRepository.save(nextJob);
                log.info("Scheduled next execution of recurring job {} at {}", job.getType(), nextRun);
            } catch (DataIntegrityViolationException duplicateSchedule) {
                log.debug(
                        "Skipped duplicate recurring schedule for type {} and cron '{}'",
                        job.getType(),
                        registration.recurringCron());
            }
        } catch (Exception cronEx) {
            log.error(
                    "Failed to reschedule recurring job {} with cron '{}'",
                    job.getType(),
                    registration.recurringCron(),
                    cronEx);
        }
    }

    private OffsetDateTime resolveNextRecurringRunAt(Job job, RegisteredJob registration, OffsetDateTime now) {
        com.jobq.annotation.Job annotation = registration.annotation();
        if (annotation == null) {
            return registration.recurringCronExpression().next(now);
        }

        OffsetDateTime anchor = job.getRunAt() != null ? job.getRunAt() : now;
        OffsetDateTime scheduledNext = registration.recurringCronExpression().next(anchor);
        if (scheduledNext == null) {
            return null;
        }
        return switch (annotation.cronMisfirePolicy()) {
            case SKIP -> registration.recurringCronExpression().next(now);
            case FIRE_ONCE -> scheduledNext.isBefore(now) ? now : scheduledNext;
            case CATCH_UP -> scheduledNext;
        };
    }

    private String recurringReplaceKey(String cron) {
        return "__jobq_recurring__:" + cron;
    }

    private void handleFailure(Job jobSnapshot, Exception exception, RegisteredJob registration) {
        OffsetDateTime now = OffsetDateTime.now();
        int currentRetryCount = jobSnapshot.getRetryCount();
        int nextRetryCount = currentRetryCount + 1;
        String errorMessage = exception.getMessage();

        if (nextRetryCount > jobSnapshot.getMaxRetries()) {
            Integer updated = transactionTemplate.execute(status -> jobRepository.markFailedTerminal(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    jobSnapshot.getLockedAt(),
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                publishDashboardRefresh(jobSnapshot.getId());
                return;
            }
        } else {
            RetryDecision retryDecision = computeRetryDecision(jobSnapshot, registration, nextRetryCount, now);
            Integer updated = transactionTemplate.execute(status -> jobRepository.markForRetry(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    retryDecision.nextRunAt(),
                    retryDecision.nextPriority(),
                    jobSnapshot.getLockedAt(),
                    nodeId));
            if (toAffectedRows(updated) > 0) {
                publishDashboardRefresh(jobSnapshot.getId());
                return;
            }
        }

        log.debug("Falling back to entity failure update for job {}", jobSnapshot.getId());
        fallbackFailureUpdate(jobSnapshot.getId(), jobSnapshot.getLockedAt(), exception, registration);
    }

    private void fallbackFailureUpdate(
            UUID jobId, OffsetDateTime expectedLockedAt, Exception exception, RegisteredJob registration) {
        transactionTemplate.executeWithoutResult(
                status -> jobRepository.findById(jobId).ifPresent(job -> {
                    if (!isMutableProcessingJob(job, expectedLockedAt)) {
                        log.debug("Skipping failure fallback for job {} due lifecycle/lock mismatch", jobId);
                        return;
                    }
                    OffsetDateTime now = OffsetDateTime.now();
                    job.setErrorMessage(exception.getMessage());
                    job.incrementRetryCount();
                    job.setUpdatedAt(now);

                    if (job.getRetryCount() > job.getMaxRetries()) {
                        if (job.getProcessingStartedAt() == null) {
                            job.setProcessingStartedAt(now);
                        }
                        job.setFailedAt(now);
                        job.setFinishedAt(null);
                        job.setLockedAt(null);
                        job.setLockedBy(null);
                    } else {
                        RetryDecision retryDecision = computeRetryDecision(job, registration, job.getRetryCount(), now);
                        job.setProcessingStartedAt(null);
                        job.setFinishedAt(null);
                        job.setFailedAt(null);
                        job.setLockedAt(null);
                        job.setLockedBy(null);
                        job.setRunAt(retryDecision.nextRunAt());
                        job.setPriority(retryDecision.nextPriority());
                    }
                    jobRepository.save(job);
                    publishDashboardRefresh(jobId);
                }));
    }

    private void handleTimeout(Job jobSnapshot, RegisteredJob registration, long maxExecutionMs) {
        OffsetDateTime now = OffsetDateTime.now();
        int currentRetryCount = jobSnapshot.getRetryCount();
        int nextRetryCount = currentRetryCount + 1;
        String errorMessage = "Execution timed out after " + maxExecutionMs + " ms";

        if (nextRetryCount > jobSnapshot.getMaxRetries()) {
            Integer updated = transactionTemplate.execute(status -> jobRepository.markTimedOutTerminal(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    jobSnapshot.getLockedAt()));
            if (toAffectedRows(updated) > 0) {
                log.warn("Marked timed-out job {} as FAILED", jobSnapshot.getId());
                publishDashboardRefresh(jobSnapshot.getId());
                return;
            }
        } else {
            RetryDecision retryDecision = computeRetryDecision(jobSnapshot, registration, nextRetryCount, now);
            Integer updated = transactionTemplate.execute(status -> jobRepository.markTimedOutForRetry(
                    jobSnapshot.getId(),
                    currentRetryCount,
                    nextRetryCount,
                    errorMessage,
                    now,
                    retryDecision.nextRunAt(),
                    retryDecision.nextPriority(),
                    jobSnapshot.getLockedAt()));
            if (toAffectedRows(updated) > 0) {
                log.warn("Requeued timed-out job {} for retry {}", jobSnapshot.getId(), nextRetryCount);
                publishDashboardRefresh(jobSnapshot.getId());
            }
        }
    }

    private RetryDecision computeRetryDecision(
            Job job, RegisteredJob registration, int retryCount, OffsetDateTime now) {
        com.jobq.annotation.Job jobAnnotation = registration.annotation();
        int nextPriority = job.getPriority();
        OffsetDateTime nextRunAt;

        if (jobAnnotation != null) {
            long delayMs = (long)
                    (jobAnnotation.initialBackoffMs() * Math.pow(jobAnnotation.backoffMultiplier(), retryCount - 1));
            nextRunAt = now.plusNanos(delayMs * 1_000_000);

            if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.LOWER_ON_RETRY) {
                nextPriority = job.getPriority() - 1;
            } else if (jobAnnotation.retryPriority() == com.jobq.annotation.Job.RetryPriority.HIGHER_ON_RETRY) {
                nextPriority = job.getPriority() + 1;
            }
            return new RetryDecision(nextRunAt, nextPriority);
        }

        long delaySeconds = (long) Math.pow(properties.getJobs().getRetryBackOffTimeSeed(), Math.max(1, retryCount));
        nextRunAt = now.plusSeconds(delaySeconds);
        return new RetryDecision(nextRunAt, nextPriority);
    }

    private boolean isExpectedException(RegisteredJob registration, Exception exception) {
        com.jobq.annotation.Job jobAnnotation = registration.annotation();
        if (jobAnnotation == null || jobAnnotation.expectedExceptions().length == 0) {
            return false;
        }

        Throwable current = exception;
        while (current != null) {
            for (Class<? extends Throwable> expected : jobAnnotation.expectedExceptions()) {
                if (expected.isAssignableFrom(current.getClass())) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private int toAffectedRows(Integer updatedRows) {
        return updatedRows == null ? 0 : updatedRows;
    }

    private boolean isMutableProcessingJob(Job job, OffsetDateTime expectedLockedAt) {
        return job.getProcessingStartedAt() != null
                && job.getFinishedAt() == null
                && job.getFailedAt() == null
                && job.getCancelledAt() == null
                && job.getLockedAt() != null
                && (expectedLockedAt == null || expectedLockedAt.equals(job.getLockedAt()))
                && nodeId.equals(job.getLockedBy());
    }

    private void publishDashboardRefresh(UUID jobId) {
        if (dashboardEventBus != null) {
            dashboardEventBus.publishJobRuntime(jobId);
            dashboardEventBus.publishRefresh();
        }
    }

    private record RetryDecision(OffsetDateTime nextRunAt, int nextPriority) {}

    @FunctionalInterface
    private interface JobInvoker {
        void invoke(UUID jobId, Object payload) throws Exception;
    }

    @FunctionalInterface
    private interface PayloadDeserializer {
        Object deserialize(JsonNode rawPayload) throws Exception;
    }

    private record RegisteredJob(
            String type,
            PayloadDeserializer payloadDeserializer,
            com.jobq.annotation.Job annotation,
            JobInvoker invoker,
            JobErrorHandler errorHandler,
            JobSuccessHandler successHandler,
            JobAfterHandler afterHandler,
            String recurringCron,
            CronExpression recurringCronExpression) {}

    @FunctionalInterface
    private interface JobErrorHandler {
        void onError(UUID jobId, Object payload, Exception exception) throws Exception;
    }

    @FunctionalInterface
    private interface JobSuccessHandler {
        void onSuccess(UUID jobId, Object payload) throws Exception;
    }

    @FunctionalInterface
    private interface JobAfterHandler {
        void after(UUID jobId, Object payload) throws Exception;
    }

    @EventListener(ContextClosedEvent.class)
    void onContextClosed() {
        shutdownInfrastructure();
    }

    @PreDestroy
    void shutdownExecutor() {
        shutdownInfrastructure();
        pollingExecutor.shutdownNow();
        processingExecutor.shutdownNow();
        awaitTermination(pollingExecutor, "polling");
        awaitTermination(processingExecutor, "processing");
    }

    private void shutdownInfrastructure() {
        shuttingDown.set(true);
        localSignalSubscription.run();
        notificationListenerRunning.set(false);
        if (notificationListenerThread != null) {
            notificationListenerThread.interrupt();
        }
    }

    private void awaitTermination(java.util.concurrent.ExecutorService executor, String executorName) {
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.debug("Timed out waiting for JobQ {} executor shutdown", executorName);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
