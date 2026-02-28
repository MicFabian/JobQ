package com.jobq;

import com.jobq.config.JobQProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@ConditionalOnProperty(prefix = "jobq.database", name = "skip-create", havingValue = "false", matchIfMissing = true)
public class JobSchemaInitializer implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(JobSchemaInitializer.class);
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern VERSION_FILE_PATTERN = Pattern.compile("^V([0-9]+(?:_[0-9]+)*)__([A-Za-z0-9_\\-]+)\\.sql$");
    private static final String MIGRATION_RESOURCE_PATTERN = "classpath*:jobq/migration/V*__*.sql";
    private static final long POSTGRES_ADVISORY_LOCK_KEY = 8_244_786_782_168_701_501L;

    private final DataSource dataSource;
    private final PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
    private final String tablePrefix;
    private final boolean failOnMigrationError;

    public JobSchemaInitializer(
            DataSource dataSource,
            ObjectProvider<JobQProperties> propertiesProvider,
            Environment environment) {
        this.dataSource = dataSource;
        JobQProperties properties = propertiesProvider.getIfAvailable();
        String configuredPrefix = properties != null
                ? properties.getDatabase().getTablePrefix()
                : environment.getProperty("jobq.database.table-prefix", "");
        boolean failFast = properties != null
                ? properties.getDatabase().isFailOnMigrationError()
                : environment.getProperty("jobq.database.fail-on-migration-error", Boolean.class, true);
        this.tablePrefix = normalizePrefix(configuredPrefix);
        this.failOnMigrationError = failFast;
    }

    @Override
    public void afterPropertiesSet() {
        String migrationTable = resolveIdentifier("jobq_schema_migrations");
        String jobsTable = resolveIdentifier("jobq_jobs");

        log.info("Initializing JobQ database schema migrations using {}", migrationTable);

        try (Connection connection = dataSource.getConnection()) {
            boolean lockAcquired = acquireLockIfPostgres(connection);
            try {
                ensureMigrationTable(connection, migrationTable);
                List<MigrationScript> migrations = loadMigrationScripts();
                if (migrations.isEmpty()) {
                    throw new IllegalStateException(
                            "No JobQ migrations were found on classpath pattern " + MIGRATION_RESOURCE_PATTERN);
                }

                Map<String, AppliedMigration> appliedMigrations = loadAppliedMigrations(connection, migrationTable);
                validateChecksums(migrations, appliedMigrations);
                int appliedNow = applyPendingMigrations(connection, migrationTable, jobsTable, migrations, appliedMigrations);

                if (appliedNow == 0) {
                    log.info("JobQ schema is up to date. {} migration(s) already applied.", appliedMigrations.size());
                } else {
                    log.info("Applied {} JobQ migration(s). Schema is now up to date.", appliedNow);
                }
            } finally {
                releaseLockIfPostgres(connection, lockAcquired);
            }
        } catch (Exception e) {
            String message = "Failed to initialize JobQ database schema migrations";
            if (failOnMigrationError) {
                throw new IllegalStateException(message, e);
            }
            log.error("{} (continuing because jobq.database.fail-on-migration-error=false)", message, e);
        }
    }

    private void ensureMigrationTable(Connection connection, String migrationTable) throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS %s (
                    version VARCHAR(64) PRIMARY KEY,
                    description VARCHAR(255) NOT NULL,
                    checksum VARCHAR(64) NOT NULL,
                    installed_on TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms BIGINT NOT NULL
                )
                """.formatted(migrationTable);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<MigrationScript> loadMigrationScripts() throws IOException {
        Resource[] resources = resourceResolver.getResources(MIGRATION_RESOURCE_PATTERN);
        List<MigrationScript> scripts = new ArrayList<>(resources.length);

        for (Resource resource : resources) {
            String fileName = resource.getFilename();
            if (fileName == null) {
                continue;
            }
            Matcher matcher = VERSION_FILE_PATTERN.matcher(fileName);
            if (!matcher.matches()) {
                throw new IllegalStateException(
                        "Invalid JobQ migration filename '" + fileName + "'. Expected format: V{version}__{description}.sql");
            }

            String version = matcher.group(1);
            List<Integer> versionParts = parseVersion(version);
            String description = matcher.group(2).replace('_', ' ');
            String sql = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
            scripts.add(new MigrationScript(version, versionParts, description, fileName, sql, sha256(sql)));
        }

        scripts.sort(Comparator.comparing(MigrationScript::versionParts, JobSchemaInitializer::compareVersions));
        validateNoDuplicateVersions(scripts);
        return scripts;
    }

    private Map<String, AppliedMigration> loadAppliedMigrations(Connection connection, String migrationTable)
            throws SQLException {
        Map<String, AppliedMigration> applied = new HashMap<>();
        String sql = "SELECT version, checksum FROM " + migrationTable;
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String version = rs.getString("version");
                String checksum = rs.getString("checksum");
                applied.put(version, new AppliedMigration(version, checksum));
            }
        }
        return applied;
    }

    private void validateChecksums(List<MigrationScript> migrations, Map<String, AppliedMigration> appliedMigrations) {
        Map<String, MigrationScript> scriptsByVersion = new HashMap<>();
        for (MigrationScript migration : migrations) {
            scriptsByVersion.put(migration.version(), migration);
        }
        for (AppliedMigration appliedMigration : appliedMigrations.values()) {
            if (!scriptsByVersion.containsKey(appliedMigration.version())) {
                throw new IllegalStateException(
                        "Migration V" + appliedMigration.version()
                                + " was applied in the database but is missing from the classpath.");
            }
        }

        for (MigrationScript migration : migrations) {
            AppliedMigration applied = appliedMigrations.get(migration.version());
            if (applied == null) {
                continue;
            }
            if (!migration.checksum().equals(applied.checksum())) {
                throw new IllegalStateException(
                        "Checksum mismatch for migration V" + migration.version()
                                + ". The migration file changed after being applied.");
            }
        }
    }

    private int applyPendingMigrations(
            Connection connection,
            String migrationTable,
            String jobsTable,
            List<MigrationScript> migrations,
            Map<String, AppliedMigration> appliedMigrations) {
        int appliedCount = 0;
        for (MigrationScript migration : migrations) {
            if (appliedMigrations.containsKey(migration.version())) {
                continue;
            }
            applyMigration(connection, migrationTable, jobsTable, migration);
            appliedCount++;
        }
        return appliedCount;
    }

    private void applyMigration(Connection connection, String migrationTable, String jobsTable, MigrationScript migration) {
        boolean originalAutoCommit = true;
        long started = System.nanoTime();
        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            String resolvedSql = renderMigrationSql(migration.sql(), jobsTable);
            EncodedResource scriptResource = new EncodedResource(
                    new ByteArrayResource(resolvedSql.getBytes(StandardCharsets.UTF_8), migration.fileName()),
                    StandardCharsets.UTF_8);
            ScriptUtils.executeSqlScript(connection, scriptResource);

            long executionMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
            String insertSql = "INSERT INTO " + migrationTable
                    + " (version, description, checksum, execution_time_ms) VALUES (?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
                statement.setString(1, migration.version());
                statement.setString(2, migration.description());
                statement.setString(3, migration.checksum());
                statement.setLong(4, executionMs);
                statement.executeUpdate();
            }
            connection.commit();
            log.info("Applied JobQ migration V{} ({}) in {} ms", migration.version(), migration.description(), executionMs);
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackException) {
                log.error("Failed to rollback JobQ migration transaction", rollbackException);
            }
            throw new IllegalStateException(
                    "Failed to apply JobQ migration V" + migration.version() + " (" + migration.description() + ")", e);
        } finally {
            try {
                connection.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                log.warn("Could not restore auto-commit after JobQ migration execution", e);
            }
        }
    }

    private String renderMigrationSql(String sql, String jobsTable) {
        if (tablePrefix.isEmpty()) {
            return sql;
        }
        return sql
                .replace("jobq_jobs", jobsTable)
                .replace("idx_jobq_jobs_", tablePrefix + "idx_jobq_jobs_");
    }

    private boolean acquireLockIfPostgres(Connection connection) {
        try {
            if (!isPostgres(connection)) {
                return false;
            }
            try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_lock(?)")) {
                statement.setLong(1, POSTGRES_ADVISORY_LOCK_KEY);
                statement.execute();
            }
            return true;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to acquire JobQ schema migration lock", e);
        }
    }

    private void releaseLockIfPostgres(Connection connection, boolean lockAcquired) {
        if (!lockAcquired) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            statement.setLong(1, POSTGRES_ADVISORY_LOCK_KEY);
            statement.execute();
        } catch (SQLException e) {
            log.warn("Failed to release JobQ schema migration lock", e);
        }
    }

    private boolean isPostgres(Connection connection) throws SQLException {
        String dbName = connection.getMetaData().getDatabaseProductName();
        return dbName != null && dbName.toLowerCase(Locale.ROOT).contains("postgresql");
    }

    private String resolveIdentifier(String suffix) {
        String identifier = tablePrefix + suffix;
        if (!SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Unsupported SQL identifier for JobQ migration: " + identifier);
        }
        return identifier;
    }

    private String normalizePrefix(String configuredPrefix) {
        if (configuredPrefix == null) {
            return "";
        }
        String trimmed = configuredPrefix.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        if (!SAFE_IDENTIFIER.matcher(trimmed).matches()) {
            throw new IllegalArgumentException("Unsupported JobQ table-prefix: " + trimmed);
        }
        return trimmed;
    }

    private static List<Integer> parseVersion(String version) {
        String[] parts = version.split("_");
        List<Integer> parsed = new ArrayList<>(parts.length);
        for (String part : parts) {
            parsed.add(Integer.parseInt(part));
        }
        return parsed;
    }

    private static int compareVersions(List<Integer> left, List<Integer> right) {
        int max = Math.max(left.size(), right.size());
        for (int i = 0; i < max; i++) {
            int l = i < left.size() ? left.get(i) : 0;
            int r = i < right.size() ? right.get(i) : 0;
            if (l != r) {
                return Integer.compare(l, r);
            }
        }
        return 0;
    }

    private static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to compute migration checksum", e);
        }
    }

    private void validateNoDuplicateVersions(List<MigrationScript> scripts) {
        Map<String, String> firstFileByVersion = new HashMap<>();
        for (MigrationScript script : scripts) {
            String previousFile = firstFileByVersion.putIfAbsent(script.version(), script.fileName());
            if (previousFile != null) {
                throw new IllegalStateException(
                        "Duplicate JobQ migration version V" + script.version() + " in files " + previousFile
                                + " and " + script.fileName());
            }
        }
    }

    private record MigrationScript(
            String version,
            List<Integer> versionParts,
            String description,
            String fileName,
            String sql,
            String checksum) {
    }

    private record AppliedMigration(String version, String checksum) {
    }
}
