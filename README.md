# JobQ

JobQ is a PostgreSQL-backed background job library for Spring Boot.

It is designed for teams that want transactional job enqueueing, high concurrency, and operational simplicity without running a separate message broker.

## Highlights

- Transactional enqueueing in the same DB transaction as business writes
- Concurrent polling using `FOR UPDATE SKIP LOCKED`
- Retry handling with backoff and priority shift strategies
- Recurring jobs via cron on `@Job`
- Optional worker type auto-resolution from `@Job` (no `getJobType()` boilerplate)
- Job grouping (`groupId`) and deduplication (`replaceKey`)
- Built-in HTMX dashboard (status, payload inspection, restart failed jobs)
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
    skip-create: false
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

### 2. Implement worker

```java
import com.jobq.JobWorker;
import com.jobq.annotation.Job;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job("send-welcome-email")
public class WelcomeEmailWorker implements JobWorker<WelcomeEmailPayload> {

    @Override
    public void process(UUID jobId, WelcomeEmailPayload payload) {
        // business logic
    }

    @Override
    public Class<WelcomeEmailPayload> getPayloadClass() {
        return WelcomeEmailPayload.class;
    }
}
```

`getJobType()` is optional. If not overridden, JobQ uses `@Job("...")`.

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

If a worker throws an exception that represents an expected business outcome, whitelist it in `@Job`.

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
- Uses DB-level locking primitives for safe concurrent workers
