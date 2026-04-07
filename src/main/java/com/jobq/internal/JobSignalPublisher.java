package com.jobq.internal;

import com.jobq.config.JobQProperties;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class JobSignalPublisher {

    private static final Logger log = LoggerFactory.getLogger(JobSignalPublisher.class);
    private static final Pattern SAFE_CHANNEL = Pattern.compile("[A-Za-z0-9_]+");
    private static final String WILDCARD_PAYLOAD = "*";

    private final JdbcTemplate jdbcTemplate;
    private final boolean notifyEnabled;
    private final String channel;
    private final CopyOnWriteArrayList<Consumer<String>> localListeners = new CopyOnWriteArrayList<>();

    public JobSignalPublisher(JdbcTemplate jdbcTemplate, JobQProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.notifyEnabled = properties.getBackgroundJobServer().isNotifyEnabled();
        this.channel = normalizeChannel(properties.getBackgroundJobServer().getNotifyChannel());
    }

    public void notifyIfDue(String jobType, OffsetDateTime runAt, OffsetDateTime now) {
        if (runAt == null || !runAt.isAfter(now)) {
            notifyJobType(jobType);
        }
    }

    public void notifyJobType(String jobType) {
        if (!notifyEnabled || jobType == null || jobType.isBlank()) {
            return;
        }
        notifyPayload(jobType);
    }

    public void notifyJobTypes(Collection<String> jobTypes) {
        if (!notifyEnabled || jobTypes == null || jobTypes.isEmpty()) {
            return;
        }
        Set<String> uniqueTypes = new LinkedHashSet<>();
        for (String jobType : jobTypes) {
            if (jobType != null && !jobType.isBlank()) {
                uniqueTypes.add(jobType);
            }
        }
        if (uniqueTypes.isEmpty()) {
            return;
        }
        if (uniqueTypes.size() > 16) {
            notifyAllTypes();
            return;
        }
        for (String jobType : uniqueTypes) {
            notifyPayload(jobType);
        }
    }

    public void notifyAllTypes() {
        if (!notifyEnabled) {
            return;
        }
        notifyPayload(WILDCARD_PAYLOAD);
    }

    public Runnable registerLocalListener(Consumer<String> listener) {
        if (listener == null) {
            return () -> {};
        }
        localListeners.add(listener);
        return () -> localListeners.remove(listener);
    }

    private void notifyPayload(String payload) {
        Runnable localDispatch = () -> {
            for (Consumer<String> localListener : localListeners) {
                try {
                    localListener.accept(payload);
                } catch (Exception e) {
                    log.debug("JobQ local signal listener failed for payload '{}'", payload, e);
                }
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()
                && TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    localDispatch.run();
                }
            });
        } else {
            localDispatch.run();
        }
        try {
            jdbcTemplate.execute("SELECT pg_notify(?, ?)", (PreparedStatement preparedStatement) -> {
                bindNotification(preparedStatement, payload);
                preparedStatement.execute();
                return null;
            });
        } catch (Exception e) {
            log.debug("JobQ PostgreSQL notification failed for payload '{}'", payload, e);
        }
    }

    private void bindNotification(PreparedStatement preparedStatement, String payload) throws SQLException {
        preparedStatement.setString(1, channel);
        preparedStatement.setString(2, payload);
    }

    private String normalizeChannel(String configuredChannel) {
        String candidate = configuredChannel == null ? "" : configuredChannel.trim();
        String normalized = candidate.isEmpty() ? "jobq_jobs_available" : candidate;
        if (!SAFE_CHANNEL.matcher(normalized).matches()) {
            throw new IllegalArgumentException("Unsupported PostgreSQL notify channel name: " + normalized);
        }
        return normalized;
    }
}
