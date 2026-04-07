# JobQ

JobQ is a PostgreSQL-backed background job library for Spring Boot.

It is built for teams that want transactional background processing without running a separate broker.
If your business write and job enqueue must succeed or fail together, JobQ is designed for that path.

## Why JobQ

- Transactional enqueue inside your existing DB transaction
- PostgreSQL concurrency via `FOR UPDATE SKIP LOCKED`
- Built-in retries, backoff, priority shifting, and expected-exception completion
- Recurring jobs via `cron` on `@Job`
- Delayed and explicit-time scheduling (`initialDelayMs`, `enqueueAt`)
- Batch enqueue APIs for high-throughput inserts
- Deduplication via `replaceKey` with configurable run-at replacement behavior
- Queue-level pause/resume, concurrency, rate-limit, and cooldown controls
- PostgreSQL `LISTEN/NOTIFY` wakeups to reduce idle poll latency
- Attempt fencing with execution timeouts (`@Job(maxExecutionMs = ...)`)
- Job runtime progress + structured runtime logs via `JobRuntime`
- Built-in HTMX dashboard (filters, sorting, retry/rerun/cancel/run-now actions, details, SSE refresh)
- Optional Micrometer metrics with zero hard dependency on actuator/micrometer
- Built-in schema migration mechanism for starter upgrades

## Compatibility

- Java 25
- Spring Boot 4.x
- PostgreSQL 17+

## Installation

Use the latest release on Maven Central.

### Gradle

```groovy
implementation 'io.github.micfabian:jobq-spring-boot-starter:<latest-version>'
```

### Maven

```xml
<dependency>
  <groupId>io.github.micfabian</groupId>
  <artifactId>jobq-spring-boot-starter</artifactId>
  <version><!-- latest --></version>
</dependency>
```

## Contributor Checks

Run the dashboard smoke test to boot the test app, seed representative dashboard data, and validate the UI with a real headless browser.

Requirements:

- Docker
- Java 25
- Node.js / `npm`

Install the smoke-test dependency bundle once:

```bash
npm ci --prefix scripts
```

Run the smoke test:

```bash
./scripts/dashboard-smoke.sh
```

If the Playwright browser runtime is not installed yet, run:

```bash
JOBQ_SMOKE_INSTALL_BROWSER=1 ./scripts/dashboard-smoke.sh
```

The script writes screenshots, trace data, page HTML, and browser/network logs to `output/playwright/dashboard-smoke/`.

## Quick Start

### 1. Define payload

```java
public record WelcomeEmailPayload(String email, java.time.Instant requestedAt) {}
```

### 2. Define a job

```java
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job(payload = WelcomeEmailPayload.class) // value omitted => job type defaults to fully-qualified class name
public class WelcomeEmailJob {

    public void process(UUID jobId, WelcomeEmailPayload payload) {
        // business logic
    }

    public void onError(UUID jobId, WelcomeEmailPayload payload, Exception exception) {
        // optional
    }

    public void onSuccess(UUID jobId, WelcomeEmailPayload payload) {
        // optional
    }

    public void after(UUID jobId, WelcomeEmailPayload payload) {
        // optional, runs for both success/failure
    }
}
```

### 3. Enqueue transactionally

```java
import com.jobq.JobClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final AppUserRepository appUserRepository;
    private final JobClient jobClient;

    public UserService(AppUserRepository appUserRepository, JobClient jobClient) {
        this.appUserRepository = appUserRepository;
        this.jobClient = jobClient;
    }

    @Transactional
    public void register(String email) {
        appUserRepository.save(new AppUser(email));
        jobClient.enqueue(WelcomeEmailJob.class, new WelcomeEmailPayload(email, java.time.Instant.now()));
    }
}
```

If the transaction rolls back, the job is not persisted.
The starter auto-registers JobQ JPA packages, so no extra `@EntityScan` / `@EnableJpaRepositories` is required.

## Job Type Resolution

JobQ resolves job type in this order:

1. Explicit `@Job(value = "...")` when provided
2. Otherwise, fully-qualified job class name

This applies to both annotation-driven jobs and `JobWorker<T>` default type inference.

Use explicit `value` for stable external IDs across class/package renames.

## Defining Jobs

You can define jobs in two styles.

### Annotation-Driven Class (No Interface Required)

- Requires `@Job` on the bean class
- `payload` can be explicit (`@Job(payload = ...)`) or inferred from `process(...)`

Supported `process(...)` signatures:

- `process()`
- `process(UUID jobId)`
- `process(Payload payload)`
- `process(UUID jobId, Payload payload)`

Supported annotation-driven `onError(...)` signatures:

- `onError(Exception e)`
- `onError(Throwable e)`
- `onError(UUID jobId, Exception e)`
- `onError(UUID jobId, Throwable e)`
- `onError(Payload payload, Exception e)`
- `onError(Payload payload, Throwable e)`
- `onError(UUID jobId, Payload payload, Exception e)`
- `onError(UUID jobId, Payload payload, Throwable e)`

Supported annotation-driven `onSuccess(...)` and `after(...)` signatures:

- `onSuccess()` / `after()`
- `onSuccess(UUID jobId)` / `after(UUID jobId)`
- `onSuccess(Payload payload)` / `after(Payload payload)`
- `onSuccess(UUID jobId, Payload payload)` / `after(UUID jobId, Payload payload)`

### Job Runtime API

Inject `JobRuntime` when a job should report progress or emit structured runtime logs to the dashboard.

```java
import com.jobq.JobRuntime;
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job(payload = ImportPayload.class)
public class ImportJob {

    private final JobRuntime jobRuntime;

    public ImportJob(JobRuntime jobRuntime) {
        this.jobRuntime = jobRuntime;
    }

    public void process(UUID jobId, ImportPayload payload) {
        jobRuntime.logInfo("Starting import for " + payload.customerId());
        jobRuntime.setProgress(25, "Fetched source data");
        jobRuntime.setProgress(75, "Validated records");
        jobRuntime.logInfo("Import complete");
    }
}
```

### Annotation + `JobLifecycle<T>` (Optional Structured Contract)

Use this when you want compile-time lifecycle structure while still registering via `@Job`.

```java
import com.jobq.JobLifecycle;
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job // value omitted => type = class name, payload inferred from JobLifecycle<WelcomeEmailPayload>
public class WelcomeEmailJob implements JobLifecycle<WelcomeEmailPayload> {

    @Override
    public void process(UUID jobId, WelcomeEmailPayload payload) throws Exception {
        // business logic
    }

    @Override
    public void onError(UUID jobId, WelcomeEmailPayload payload, Exception exception) {
        // optional
    }

    @Override
    public void onSuccess(UUID jobId, WelcomeEmailPayload payload) {
        // optional
    }

    @Override
    public void after(UUID jobId, WelcomeEmailPayload payload) {
        // optional, runs for both success/failure
    }
}
```

Notes:

- `@Job(payload = ...)` remains supported and overrides payload inference when provided.
- If `@Job.value` is omitted, job type defaults to the fully-qualified class name.

### `JobWorker<T>` Interface

```java
import com.jobq.JobWorker;
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job // value omitted => type = class name
public class WelcomeEmailWorker implements JobWorker<WelcomeEmailPayload> {

    @Override
    public void process(UUID jobId, WelcomeEmailPayload payload) {
        // business logic
    }

    // getPayloadClass() is inferred from JobWorker<WelcomeEmailPayload>
    // getJobType() is inferred from @Job (value or class name fallback)
}
```

`JobWorker<T>` also supports optional callback overrides:

- `onError(UUID, T, Exception)`
- `onSuccess(UUID, T)`
- `after(UUID, T)`

## Enqueue APIs

Both type-safe class-based and string-based APIs are available.

### Common methods

```java
// default retries (resolved from @Job maxRetries when available)
jobClient.enqueue(MyJob.class, payload);
jobClient.enqueue("MY_JOB_TYPE", payload);

// explicit retries
jobClient.enqueue(MyJob.class, payload, 5);

// group + dedup key
jobClient.enqueue(MyJob.class, payload, "reports", "customer-123");

// explicit run time
jobClient.enqueueAt(MyJob.class, payload, java.time.Instant.now().plusSeconds(90));
jobClient.enqueueAt(MyJob.class, payload, java.time.OffsetDateTime.now().plusMinutes(2));

// bulk enqueue
jobClient.enqueueAll(MyJob.class, java.util.List.of(payload1, payload2, payload3));
jobClient.enqueueAllAt(MyJob.class, java.util.List.of(payload1, payload2), java.time.Instant.now().plusSeconds(30));
```

### Default retries behavior

When calling default enqueue methods (without explicit retries):

- If JobQ has metadata for the job type (`@Job`), it uses `@Job(maxRetries = ...)`
- Otherwise, it falls back to `jobq.jobs.default-number-of-retries` (default `10`)

## Reliability, Retries, and Failure Semantics

### State model

Job status is derived from lifecycle timestamps:

- `PENDING`: `processing_started_at IS NULL`, `finished_at IS NULL`, `failed_at IS NULL`
- `PROCESSING`: `processing_started_at IS NOT NULL`, `finished_at IS NULL`, `failed_at IS NULL`
- `COMPLETED`: `finished_at IS NOT NULL`
- `FAILED`: `failed_at IS NOT NULL`
- `CANCELLED`: `cancelled_at IS NOT NULL`

### Retry behavior

On a non-expected exception in `process(...)`:

- `retry_count` is incremented
- if `retry_count <= max_retries`, job returns to `PENDING` with computed backoff
- if `retry_count > max_retries`, job is marked `FAILED`

Backoff and retry priority can be configured via `@Job`:

```java
@Job(
    maxRetries = 5,
    initialBackoffMs = 1_000,
    backoffMultiplier = 2.0,
    retryPriority = Job.RetryPriority.LOWER_ON_RETRY
)
```

### Expected exception whitelist

```java
@Job(expectedExceptions = {CustomerAlreadySyncedException.class})
```

If thrown exception (or one of its causes) matches the whitelist:

- job is marked `COMPLETED`
- no retry is scheduled
- `onSuccess(...)` is still invoked

### Callback safety

- `onError(...)` exceptions are logged and do not replace original failure handling
- `onSuccess(...)` exceptions are logged and do not change completion state
- `after(...)` exceptions are logged and do not alter persisted outcome

### Execution timeout fencing

```java
@Job(maxExecutionMs = 30_000)
class RemoteSyncJob { ... }
```

If one claimed attempt runs past `maxExecutionMs`:

- the old lock token is fenced off
- `retry_count` is incremented
- the attempt is retried or terminally failed using normal retry rules
- a late completion from the timed-out attempt cannot overwrite the newer state

## Scheduling

### Initial delay

```java
@Job(initialDelayMs = 300_000) // 5 minutes
class SyncCustomerJob { ... }
```

If enqueue is called without explicit run time, `run_at` = now + initial delay.

### Explicit scheduling

`enqueueAt(...)` sets `run_at` explicitly and overrides `initialDelayMs` for that enqueue call.

### Recurring jobs

```java
@Job(cron = "0 */5 * * * *")
class CleanupJob { ... }
```

Behavior:

- On startup, JobQ ensures one active execution exists for each recurring definition
- On successful completion, JobQ schedules the next run from cron expression

Recurring jobs can also define how missed runs are handled after downtime:

```java
@Job(
    cron = "0 */5 * * * *",
    cronMisfirePolicy = Job.CronMisfirePolicy.CATCH_UP,
    maxCatchUpExecutions = 12
)
class CleanupJob { ... }
```

Options:

- `SKIP`: jump to the next future cron occurrence
- `FIRE_ONCE`: enqueue one immediate recovery execution, then continue normally
- `CATCH_UP`: backfill missed executions sequentially up to `maxCatchUpExecutions`

## Deduplication and Grouping

### Grouping (`groupId`)

`groupId` is persisted and visible in the dashboard for routing/filtering use cases.

Grouped scheduling behavior is configurable via `@Job.groupDelayPolicy`:

```java
@Job(groupDelayPolicy = Job.GroupDelayPolicy.SYNC_WITH_NEW_DELAY)
class SyncGroupJob { ... }
```

Options:

- `SYNC_WITH_NEW_DELAY`:
  enqueueing a grouped job updates `run_at` for all active pending jobs of the same type/group to the new enqueue schedule.
- `KEEP_EXISTING_DELAY_RUN_ALL_ON_FIRST_DUE` (default):
  grouped jobs keep their own delays, but when the first grouped job becomes due, JobQ releases the remaining pending jobs in that group to run immediately.

### Deduplication (`replaceKey`)

```java
jobClient.enqueue(MyJob.class, payload, "reports", "customer-123");
```

Dedup applies only to active pending rows (`processing_started_at IS NULL`, `finished_at IS NULL`, `failed_at IS NULL`).

- If matching pending row exists, JobQ updates that row and returns its ID
- If matching row is already processing/terminal, JobQ inserts a new row

When replacing a pending row, `run_at` behavior is controlled by:

```java
@Job(deduplicationRunAtPolicy = Job.DeduplicationRunAtPolicy.KEEP_EXISTING)
```

Options:

- `UPDATE_ON_REPLACE` (default)
- `KEEP_EXISTING`

## Dashboard

Enable dashboard:

```yaml
jobq:
  dashboard:
    enabled: true
```

Default path: `/jobq/dashboard` (configurable).

### Features

- Live lifecycle cards and paged job table
- Search (type/group/replace key/full UUID)
- Status filter, sorting, page-size control
- Scheduled-only and retried-only filters
- Silent polling with optional auto-refresh toggle
- Reliability column (`retryCount / maxRetries`)
- Failure details preview in list + full detail panel
- Runtime panel with job progress and runtime logs
- Retry failed jobs, rerun completed jobs, cancel pending jobs, and run delayed jobs immediately
- Batch actions across the active filter: rerun selected, rerun filtered failures since a timestamp, run filtered delayed jobs now, cancel filtered jobs
- Operational panels for queue controls, recurring jobs, worker nodes, metrics, and dashboard audit events

### Queue Controls

The dashboard can persist per-job-type operational controls without redeploying the application:

- pause / resume dispatch
- max concurrency
- rate limit per minute
- dispatch cooldown in milliseconds

These controls are stored in `jobq_queue_controls` so they survive restarts.

### Read-Only Mode and Payload Redaction

For production support or observer users, the dashboard can be switched to read-only mode:

```yaml
jobq:
  dashboard:
    read-only: true
```

Write actions remain server-side blocked even if a client attempts to invoke them directly.

Payload fields can also be redacted before they are rendered in the details drawer:

```yaml
jobq:
  dashboard:
    redacted-payload-fields:
      - password
      - token
      - accessKey
```

### Security modes

Dashboard is never open when enabled.

#### `BASIC` (default)

- If `username` + `password` are set, those credentials are required
- If both are set, no generated credentials are logged
- If either is missing, JobQ generates random credentials at startup and logs them once

#### `SPRING_SECURITY`

- JobQ Basic Auth is bypassed
- Your Spring Security authentication is used
- Access requires `jobq.dashboard.required-role` (default `JOBQ_DASHBOARD`)

```yaml
jobq:
  dashboard:
    enabled: true
    auth-mode: SPRING_SECURITY
    required-role: JOBQ_DASHBOARD
```

## Metrics

If Micrometer is available, JobQ registers:

- `jobq.jobs.total`
- `jobq.jobs.count{status="PENDING|PROCESSING|COMPLETED|FAILED"}`

If Micrometer is absent, JobQ still starts normally.

## Configuration Reference

```yaml
jobq:
  jobs:
    default-number-of-retries: 10
    retry-back-off-time-seed: 3

  background-job-server:
    enabled: true
    worker-count: 4
    poll-interval-in-seconds: 15
    notify-enabled: true
    notify-channel: jobq_jobs_available
    notify-listen-timeout-ms: 1000
    execution-timeout-check-interval-in-seconds: 30
    node-heartbeat-interval-in-seconds: 10
    delete-succeeded-jobs-after: 36h
    permanently-delete-deleted-jobs-after: 72h

  database:
    table-prefix: ""
    skip-create: false
    fail-on-migration-error: true

  dashboard:
    enabled: false
    path: /jobq/dashboard
    auth-mode: BASIC               # BASIC | SPRING_SECURITY
    required-role: JOBQ_DASHBOARD  # SPRING_SECURITY mode
    username: ""                  # BASIC mode
    password: ""                  # BASIC mode
    read-only: false
    redacted-payload-fields: []
```

## Schema and Migrations

JobQ includes built-in versioned SQL migrations from:

- `classpath:jobq/migration/V{version}__{description}.sql`

Migration behavior:

- history table: `jobq_schema_migrations` (prefix-aware)
- checksum validation on startup
- pending versions are applied in order
- PostgreSQL advisory lock to avoid multi-node race during migration

Main job table: `jobq_jobs` (prefix-aware).

Runtime tuning included by default:

- queue-table storage settings for update-heavy workloads
- timeout lookup index for stuck-attempt rescue
- trigram search indexes for dashboard filter/search performance

Disable built-in migrations if your platform manages DDL externally:

```yaml
jobq:
  database:
    skip-create: true
```

Continue startup even if migration fails (not recommended):

```yaml
jobq:
  database:
    fail-on-migration-error: false
```

## Testing and Performance Tasks

```bash
./gradlew test
./gradlew loadTest
./gradlew perfTest
```

- `loadTest`: stress scenarios (concurrency, dedup contention, retries)
- `perfTest`: benchmark-oriented throughput/timing scenarios, including batch enqueue

## Publishing (Maintainers)

Release:

```bash
./gradlew publishToMavenCentral -PreleaseVersion=<version>
```

Snapshot:

```bash
./gradlew publishToMavenCentral -PreleaseVersion=<version>-SNAPSHOT
```

GitHub Actions:

- CI: `.github/workflows/ci.yml` (`test` on push/PR)
- Release: `.github/workflows/release.yml` (tag/manual publish flow)
