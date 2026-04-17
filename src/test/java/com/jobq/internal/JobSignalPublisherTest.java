package com.jobq.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.jobq.config.JobQProperties;
import java.sql.PreparedStatement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@SuppressWarnings("unchecked")
class JobSignalPublisherTest {

    @AfterEach
    void tearDownTransactionSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void shouldNotifyLocalListenersImmediatelyOutsideTransaction() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        AtomicReference<String> payload = new AtomicReference<>();
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, enabledProperties());
        List<String> localPayloads = new ArrayList<>();
        publisher.registerLocalListener(localPayloads::add);

        stubPgNotify(jdbcTemplate, preparedStatement, payload);

        publisher.notifyJobType("TYPE_A");

        assertEquals(List.of("TYPE_A"), localPayloads);
        assertEquals("TYPE_A", payload.get());
        verify(preparedStatement).setString(1, "jobq_jobs_available");
        verify(preparedStatement).setString(2, "TYPE_A");
    }

    @Test
    void shouldDeferLocalListenersUntilAfterCommitWhenTransactionIsActive() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        AtomicReference<String> payload = new AtomicReference<>();
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, enabledProperties());
        List<String> localPayloads = new ArrayList<>();
        publisher.registerLocalListener(localPayloads::add);

        stubPgNotify(jdbcTemplate, preparedStatement, payload);

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            publisher.notifyJobType("TYPE_B");

            assertTrue(localPayloads.isEmpty());
            assertEquals(
                    1, TransactionSynchronizationManager.getSynchronizations().size());
            assertEquals("TYPE_B", payload.get());

            TransactionSynchronizationManager.getSynchronizations().forEach(sync -> sync.afterCommit());

            assertEquals(List.of("TYPE_B"), localPayloads);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void shouldNotifyWildcardWhenTooManyDistinctTypesAreRequested() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        AtomicReference<String> payload = new AtomicReference<>();
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, enabledProperties());

        stubPgNotify(jdbcTemplate, preparedStatement, payload);

        List<String> jobTypes = new ArrayList<>();
        for (int index = 0; index < 17; index++) {
            jobTypes.add("TYPE_" + index);
        }

        publisher.notifyJobTypes(jobTypes);

        assertEquals("*", payload.get());
        verify(preparedStatement).setString(2, "*");
    }

    @Test
    void shouldIgnoreBlankTypesAndFutureRunAt() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, enabledProperties());

        publisher.notifyJobType("   ");
        publisher.notifyJobTypes(java.util.Arrays.asList(" ", null, "   "));
        publisher.notifyIfDue("FUTURE_JOB", OffsetDateTime.now().plusMinutes(5), OffsetDateTime.now());

        verify(jdbcTemplate, never())
                .execute(
                        eq("SELECT pg_notify(?, ?)"),
                        org.mockito.ArgumentMatchers.<PreparedStatementCallback<Object>>any());
    }

    @Test
    void shouldDeduplicateJobTypesAndIgnoreBlankEntries() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, enabledProperties());
        List<String> payloads = new ArrayList<>();

        doAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    PreparedStatementCallback<Object> callback = invocation.getArgument(1);
                    callback.doInPreparedStatement(preparedStatement);
                    return null;
                })
                .when(jdbcTemplate)
                .execute(
                        eq("SELECT pg_notify(?, ?)"),
                        org.mockito.ArgumentMatchers.<PreparedStatementCallback<Object>>any());
        doAnswer(invocation -> {
                    payloads.add(invocation.getArgument(1));
                    return null;
                })
                .when(preparedStatement)
                .setString(eq(2), any(String.class));

        publisher.notifyJobTypes(java.util.Arrays.asList("TYPE_A", " ", null, "TYPE_A", "TYPE_B"));

        assertEquals(List.of("TYPE_A", "TYPE_B"), payloads);
    }

    @Test
    void shouldIgnoreNotificationsWhenDisabled() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobQProperties properties = new JobQProperties();
        properties.getBackgroundJobServer().setNotifyEnabled(false);
        JobSignalPublisher publisher = new JobSignalPublisher(jdbcTemplate, properties);

        publisher.notifyJobType("TYPE_A");
        publisher.notifyAllTypes();

        verify(jdbcTemplate, never())
                .execute(
                        eq("SELECT pg_notify(?, ?)"),
                        org.mockito.ArgumentMatchers.<PreparedStatementCallback<Object>>any());
    }

    @Test
    void shouldRejectInvalidNotifyChannelName() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobQProperties properties = new JobQProperties();
        properties.getBackgroundJobServer().setNotifyEnabled(true);
        properties.getBackgroundJobServer().setNotifyChannel("bad-channel-name!");

        IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> new JobSignalPublisher(jdbcTemplate, properties));
        assertTrue(error.getMessage().contains("Unsupported PostgreSQL notify channel name"));
    }

    private void stubPgNotify(
            JdbcTemplate jdbcTemplate, PreparedStatement preparedStatement, AtomicReference<String> payloadCapture)
            throws Exception {
        doAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    PreparedStatementCallback<Object> callback = invocation.getArgument(1);
                    callback.doInPreparedStatement(preparedStatement);
                    return null;
                })
                .when(jdbcTemplate)
                .execute(eq("SELECT pg_notify(?, ?)"), any(PreparedStatementCallback.class));

        doAnswer(invocation -> {
                    Object value = invocation.getArgument(1);
                    if ("TYPE_A".equals(value) || "TYPE_B".equals(value) || "*".equals(value)) {
                        payloadCapture.set((String) value);
                    }
                    return null;
                })
                .when(preparedStatement)
                .setString(eq(2), any(String.class));
    }

    private JobQProperties enabledProperties() {
        JobQProperties properties = new JobQProperties();
        properties.getBackgroundJobServer().setNotifyEnabled(true);
        return properties;
    }
}
