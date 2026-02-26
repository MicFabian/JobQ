# JobQ

JobQ is a PostgreSQL-backed background job library for Spring Boot.

It is designed for teams that want transactional job enqueueing, high concurrency, and operational simplicity without running a separate message broker.

## Highlights

- Transactional enqueueing in the same DB transaction as business writes
- Concurrent polling using `FOR UPDATE SKIP LOCKED`
- Retry handling with backoff and priority shift strategies
- Recurring jobs via cron on `@Job`
- Optional job type auto-resolution from `@Job` (no `getJobType()` boilerplate)
- Annotation-driven jobs with `@Job(payload = ...)` (no interface required)
- Optional payload type auto-resolution from `JobWorker<T>` when using the interface
- Optional `onError(...)` hook per job
- Optional `onSuccess(...)` / `after(...)` hook per job
- Job grouping (`groupId`) and deduplication (`replaceKey`)
- Built-in HTMX dashboard (status, payload inspection, restart failed jobs)
- Dashboard/metrics lifecycle counters fetched via single aggregated query
- Built-in Micrometer gauges
- Automatic retention cleanup for completed/failed jobs
- Java 25 and Spring Boot 4 support

## Requirements

- Java 25
- Spring Boot 4
- PostgreSQL 17+

## Installation

```groovy
implementation 'com.jobq:jobq-spring-boot-starter:1.0.0'
```

## Configuration

```yaml
jobq:
  background-job-server:
    enabled: true
    worker-count: 4
    poll-interval-in-seconds: 15
    delete-succeeded-jobs-after: 36h
    permanently-delete-deleted-jobs-after: 72h

  database:
    skip-create: false            # true = do not auto-run jobq-schema.sql
    table-prefix: ""

  dashboard:
    enabled: false
    path: "/jobq/dashboard"
    auth-mode: BASIC              # BASIC | SPRING_SECURITY
    required-role: JOBQ_DASHBOARD # used only in SPRING_SECURITY mode
    username: ""                  # used only in BASIC mode
    password: ""                  # used only in BASIC mode
```

## Quick Start

### 1. Define payload

```java
public record WelcomeEmailPayload(String email, String template) {}
```

### 2. Implement job

```java
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job(value = "send-welcome-email", payload = WelcomeEmailPayload.class)
public class WelcomeEmailJob {

    public void process(UUID jobId, WelcomeEmailPayload payload) {
        // business logic
    }

    public void onError(UUID jobId, WelcomeEmailPayload payload, Exception exception) {
        // optional: called when process(...) throws
    }

    public void onSuccess(UUID jobId, WelcomeEmailPayload payload) {
        // optional: called after successful completion
    }
}
```

For annotation-only jobs, the job type comes from `@Job(value = "...")`.
`JobWorker` is optional. If you use `JobWorker<T>`, `getJobType()` and `getPayloadClass()` are optional and inferred.
For `JobWorker<T>`, you can override `onSuccess(...)` and/or `after(...)`.

Supported annotation-driven `process` signatures:

- `process()`
- `process(UUID jobId)`
- `process(Payload payload)`
- `process(UUID jobId, Payload payload)`

Supported annotation-driven `onError` signatures:

- `onError(Exception e)`
- `onError(UUID jobId, Exception e)`
- `onError(Payload payload, Exception e)`
- `onError(UUID jobId, Payload payload, Exception e)`

If `onError(...)` throws, JobQ logs that callback failure and continues normal retry/failure handling using the original processing exception.

Supported annotation-driven `onSuccess` signatures:

- `onSuccess()`
- `onSuccess(UUID jobId)`
- `onSuccess(Payload payload)`
- `onSuccess(UUID jobId, Payload payload)`

`onSuccess(...)` is called only when the job finishes successfully.

If a success callback throws, JobQ logs it and keeps the job in `COMPLETED` state.

Supported annotation-driven `after` signatures:

- `after()`
- `after(UUID jobId)`
- `after(Payload payload)`
- `after(UUID jobId, Payload payload)`

`after(...)` is called after job execution regardless of success or failure.
If `after(...)` throws, JobQ logs it and keeps the persisted job outcome unchanged.

### 3. Enqueue inside a transaction

```java
import com.jobq.JobClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final JobClient jobClient;

    public UserService(JobClient jobClient) {
        this.jobClient = jobClient;
    }

    @Transactional
    public void register(String email) {
        // save business data...
        jobClient.enqueue("send-welcome-email", new WelcomeEmailPayload(email, "default"));
    }
}
```

If the transaction rolls back, the job is not persisted.

## Schema Initialization

By default, JobQ auto-runs `jobq-schema.sql` at startup.

Disable schema auto-creation when your project manages DDL externally:

```yaml
jobq:
  database:
    skip-create: true
```

## Retry, Backoff, Priority

```java
@Job(
    value = "sync-customer",
    maxRetries = 5,
    initialBackoffMs = 1000,
    backoffMultiplier = 2.0,
    retryPriority = Job.RetryPriority.LOWER_ON_RETRY
)
```

When processing throws a non-whitelisted exception:

- `retryCount` is incremented
- job is moved back to `PENDING` with backoff (until retries are exhausted)
- job becomes `FAILED` when retries are exceeded

## Expected Exceptions (Whitelist)

If a job throws an exception that represents an expected business outcome, whitelist it in `@Job`.

```java
@Job(
    value = "sync-customer",
    expectedExceptions = {CustomerAlreadySyncedException.class}
)
```

If one of these exceptions is thrown:

- job is marked `COMPLETED`
- `retryCount` is not incremented
- no retry is scheduled

## Recurring Jobs

```java
@Job(value = "cleanup-temp-files", cron = "0 0 * * * *")
```

Behavior:

- On startup, JobQ bootstraps recurring jobs if no active execution exists
- After a successful run, JobQ schedules the next run from the cron expression

## Grouping and Deduplication

### Grouping

```java
jobClient.enqueue("generate-report", payload, "reports");
```

### Deduplication (`replaceKey`)

```java
jobClient.enqueue("generate-report", payload, "reports", "customer-123");
```

If a `PENDING` job exists with the same `(type, replaceKey)`, JobQ updates that existing row instead of creating another.

## Dashboard

Enable with:

```yaml
jobq:
  dashboard:
    enabled: true
```

Default route: `/jobq/dashboard` (configurable with `jobq.dashboard.path`).

Features:

- live stats
- paged job listing
- payload/details view
- restart failed jobs

### Dashboard Security (Never Open)

JobQ dashboard endpoints are always protected when dashboard is enabled.

#### Mode 1: `BASIC` (default)

- If both `jobq.dashboard.username` and `jobq.dashboard.password` are set:
  - those credentials are used
  - no startup credential log is emitted
- If either is missing:
  - JobQ generates random username/password at startup
  - generated credentials are logged once at startup

#### Mode 2: `SPRING_SECURITY`

- JobQ does not use Basic Auth credentials in this mode
- Request must be authenticated by your project’s Spring Security setup
- Authenticated user must have `jobq.dashboard.required-role` (default: `JOBQ_DASHBOARD`)
  - role value may be configured with or without `ROLE_` prefix

Example:

```yaml
jobq:
  dashboard:
    enabled: true
    auth-mode: SPRING_SECURITY
    required-role: JOBQ_DASHBOARD
```

In this mode, no credentials are auto-generated or logged by JobQ.

## Metrics

When Micrometer is on the classpath, JobQ registers:

- `jobq.jobs.total`
- `jobq.jobs.count{status="PENDING|PROCESSING|COMPLETED|FAILED"}`

## Schema

JobQ can auto-initialize schema from `jobq-schema.sql`.

Main table: `jobq_jobs`

Important columns:

- `processing_started_at`, `finished_at`, `failed_at`
- `retry_count`, `max_retries`, `priority`, `run_at`
- `group_id`, `replace_key`, `cron`

Important indexes:

- polling index for efficient lock/poll
- group index
- unique partial index on `(type, replace_key)` for `PENDING` rows

## Design Notes

- No external broker required
- Works well with multiple app instances
- Uses DB-level locking primitives for safe concurrent processors
