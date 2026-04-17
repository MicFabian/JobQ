package com.jobq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jobq.config.JobQProperties;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

class JobSchemaInitializerTest {

    @Test
    void shouldRenderPrefixedIdentifiersInMigrationSql() {
        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setTablePrefix("tenant_");

        JobSchemaInitializer initializer =
                new JobSchemaInitializer(mockDataSource(), provider(properties), new MockEnvironment());

        String rendered = ReflectionTestUtils.invokeMethod(
                initializer,
                "renderMigrationSql",
                """
                CREATE TABLE jobq_jobs ();
                CREATE TABLE jobq_queue_controls ();
                CREATE TABLE jobq_job_logs ();
                CREATE TABLE jobq_worker_nodes ();
                CREATE TABLE jobq_dashboard_audit_log ();
                CREATE INDEX idx_jobq_jobs_polling ON jobq_jobs(id);
                """,
                "tenant_jobq_jobs");

        assertTrue(rendered.contains("tenant_jobq_jobs"));
        assertTrue(rendered.contains("tenant_jobq_queue_controls"));
        assertTrue(rendered.contains("tenant_jobq_job_logs"));
        assertTrue(rendered.contains("tenant_jobq_worker_nodes"));
        assertTrue(rendered.contains("tenant_jobq_dashboard_audit_log"));
        assertTrue(rendered.contains("idx_tenant_jobq_jobs_polling"));
    }

    @Test
    void shouldRejectUnsupportedTablePrefix() {
        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setTablePrefix("bad-prefix");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new JobSchemaInitializer(mockDataSource(), provider(properties), new MockEnvironment()));

        assertTrue(exception.getMessage().contains("Unsupported JobQ table-prefix"));
    }

    @Test
    void shouldThrowWhenConnectionFailsAndFailFastIsEnabled() throws SQLException {
        DataSource dataSource = mockDataSource();
        org.mockito.Mockito.when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setFailOnMigrationError(true);

        JobSchemaInitializer initializer =
                new JobSchemaInitializer(dataSource, provider(properties), new MockEnvironment());

        IllegalStateException exception = assertThrows(IllegalStateException.class, initializer::afterPropertiesSet);
        assertTrue(exception.getMessage().contains("Failed to initialize JobQ database schema migrations"));
        assertTrue(exception.getCause() instanceof SQLException);
    }

    @Test
    void shouldContinueWhenConnectionFailsAndFailFastIsDisabled() throws SQLException {
        DataSource dataSource = mockDataSource();
        org.mockito.Mockito.when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

        JobQProperties properties = new JobQProperties();
        properties.getDatabase().setFailOnMigrationError(false);

        JobSchemaInitializer initializer =
                new JobSchemaInitializer(dataSource, provider(properties), new MockEnvironment());

        assertDoesNotThrow(initializer::afterPropertiesSet);
    }

    @Test
    void shouldRejectAppliedMigrationThatIsMissingFromClasspath() throws Exception {
        JobSchemaInitializer initializer =
                new JobSchemaInitializer(mockDataSource(), provider(null), new MockEnvironment());

        Object migration = migrationScript("1", "V1__baseline.sql", "SELECT 1", "checksum-1");
        Object appliedMigration = appliedMigration("2", "checksum-2");

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> ReflectionTestUtils.invokeMethod(
                        initializer, "validateChecksums", List.of(migration), Map.of("2", appliedMigration)));

        assertTrue(exception.getMessage().contains("missing from the classpath"));
    }

    @Test
    void shouldRejectChecksumMismatchForAppliedMigration() throws Exception {
        JobSchemaInitializer initializer =
                new JobSchemaInitializer(mockDataSource(), provider(null), new MockEnvironment());

        Object migration = migrationScript("1", "V1__baseline.sql", "SELECT 1", "checksum-1");
        Object appliedMigration = appliedMigration("1", "different-checksum");

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> ReflectionTestUtils.invokeMethod(
                        initializer, "validateChecksums", List.of(migration), Map.of("1", appliedMigration)));

        assertTrue(exception.getMessage().contains("Checksum mismatch for migration V1"));
    }

    @Test
    void shouldReadTablePrefixFromEnvironmentWhenPropertiesAreUnavailable() {
        MockEnvironment environment = new MockEnvironment().withProperty("jobq.database.table-prefix", "env_");

        JobSchemaInitializer initializer = new JobSchemaInitializer(mockDataSource(), provider(null), environment);

        String rendered = ReflectionTestUtils.invokeMethod(
                initializer,
                "renderMigrationSql",
                "CREATE INDEX idx_jobq_jobs_polling ON jobq_jobs(id)",
                "env_jobq_jobs");

        assertEquals("CREATE INDEX idx_env_jobq_jobs_polling ON env_jobq_jobs(id)", rendered);
    }

    private static Object migrationScript(String version, String fileName, String sql, String checksum)
            throws Exception {
        Class<?> type = Class.forName("com.jobq.JobSchemaInitializer$MigrationScript");
        Constructor<?> constructor = type.getDeclaredConstructor(
                String.class, List.class, String.class, String.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(version, parseVersion(version), "desc", fileName, sql, checksum);
    }

    private static Object appliedMigration(String version, String checksum) throws Exception {
        Class<?> type = Class.forName("com.jobq.JobSchemaInitializer$AppliedMigration");
        Constructor<?> constructor = type.getDeclaredConstructor(String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(version, checksum);
    }

    private static List<Integer> parseVersion(String version) {
        return Arrays.stream(version.split("_")).map(Integer::parseInt).toList();
    }

    private static DataSource mockDataSource() {
        return org.mockito.Mockito.mock(DataSource.class);
    }

    private static <T> ObjectProvider<T> provider(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject(Object... args) {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public T getIfUnique() {
                return value;
            }

            @Override
            public T getObject() {
                return value;
            }
        };
    }
}
