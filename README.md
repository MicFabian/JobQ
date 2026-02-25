<div align="center">
  <h1>JobQ 🚀</h1>
  <p><b>A highly reliable, zero-dependency PostgreSQL Background Job Processing Library for Spring Boot 4</b></p>
</div>

JobQ acts as a robust, lightweight replacement for heavy message brokers like RabbitMQ or Redis by leveraging your existing PostgreSQL database to manage background jobs cleanly and transactionally. 

Built natively for **Java 25** and **Spring Boot 4**, JobQ automatically takes advantage of PostgreSQL 17 features like `SKIP LOCKED` for highly concurrent queue polling without deadlocks.

---

## 🌟 Key Features

* **Transactional Consistency**: Save your business data and enqueue a job in the precise same atomic transaction. 
* **Concurrent & Scalable**: Multiple application instances safely poll jobs simultaneously using PostgreSQL `FOR UPDATE SKIP LOCKED`.
* **Advanced Routing & Resiliency**: Native support for configurable max retries, fixed/exponential backoffs, and dynamic priority shifting when jobs fail.
* **Premium HTMX Dashboard**: A completely built-in, standalone glass-morphic web UI complete with live polling, pagination, payload inspection, and security.
* **Production-Grade Observability**: Turnkey Micrometer `Gauge` metrics natively export your queue statistics to tools like Prometheus.

---

## 📦 Installation

Add the dependency to your `build.gradle` or `pom.xml`. Since JobQ is a well-behaved Spring Boot Auto-Configuration Starter, you don't need any complex setup out of the box.

**Gradle:**
```groovy
implementation 'com.jobq:jobq-spring-boot-starter:1.0.0'
```

---

## 🚀 Quick Start Guide

### 1. Configuration (Optional)
JobQ comes with sensible zero-configuration defaults, including automatic database schema initialization! If you ever need to tune the engine, just open `application.yml`:

```yaml
jobq:
  background-job-server:
    enabled: true                  # Set to false to disable polling on this node
    worker-count: 4                # Number of concurrent async threads 
    poll-interval-in-seconds: 15   # How frequently the local node checks the database
    delete-succeeded-jobs-after: 36h
    permanently-delete-deleted-jobs-after: 72h
  
  database:
    skip-create: false             # Set to true if managing schema with Flyway/Liquibase
    table-prefix: "my_app_"        # Optional prefix for JobQ tables
    
  dashboard:
    enabled: true                  # Access at /jobq/dashboard 
    path: "/jobq/dashboard"
    username: "admin"              # Simple HTTP Basic Auth protection
    password: "supersecretpassword"
```

### 2. Define a Payload
Jobs execute logic based on data (payloads). Payloads can be any valid POJO or Java Record. JobQ handles JSON serialization automatically.

```java
public record WelcomeEmailPayload(String emailAddress, String templateId) {}
```


### 3. Creating a Job Worker

Implement the `JobWorker<T>` interface and annotate your bean with `@Job` to process the jobs. The `@Job` annotation allows you to fine-tune exactly how the job behaves in the event of failures!

```java
import com.jobq.JobWorker;
import com.jobq.annotation.Job;
import com.jobq.annotation.Job.RetryPriority;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Job(
    value = "SEND_WELCOME_EMAIL", 
    maxRetries = 5,                 // Try 5 times before failing permanently
    initialBackoffMs = 2000,        // Wait 2 seconds before the first retry
    backoffMultiplier = 2.0,        // Exponential backoff: 2s, 4s, 8s, 16s...
    retryPriority = RetryPriority.LOWER_ON_RETRY // Deprioritize failing jobs behind healthy ones!
)
public class WelcomeEmailWorker implements JobWorker<WelcomeEmailPayload> {

    @Override
    public String getJobType() {
        return "SEND_WELCOME_EMAIL";
    }

    @Override
    public Class<WelcomeEmailPayload> getPayloadClass() {
        return WelcomeEmailPayload.class;
    }

    @Override
    public void process(UUID jobId, WelcomeEmailPayload payload) throws Exception {
        // Your business logic goes here!
        // If this method throws any Exception, JobQ will catch it, log the error message,
        // unlock the database row, and schedule the job for a retry using the backoff algorithm.
        System.out.println("Sending email to: " + payload.emailAddress());
    }
}
```


### 4. Enqueuing a Job

Simply inject `JobClient` into your services. The real power comes when you call `enqueue` inside an `@Transactional` block. If the transaction rolls back, the job is never scheduled!

```java
import com.jobq.JobClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final JobClient jobClient;
    private final UserRepository userRepository;

    public UserService(JobClient jobClient, UserRepository userRepository) {
        this.jobClient = jobClient;
        this.userRepository = userRepository;
    }

    @Transactional
    public void registerNewUser(String email) {
        // 1. Save business data
        User user = new User(email);
        userRepository.save(user);

        // 2. Enqueue the background job atomically!
        WelcomeEmailPayload payload = new WelcomeEmailPayload(email, "WELCOME_V1");
        jobClient.enqueue("SEND_WELCOME_EMAIL", payload);
    }
}
```

---

## 🛠️ Advanced Functionalities

### Advanced Scheduling & Prioritization
While `JobClient.enqueue(type, payload)` handles the most common use cases and inherits defaults from standard properties, you can also inject `JobClient` and override the number of retries explicitly using `enqueue(String type, Object payload, int maxRetries)`.

Need to run a job in the future? Inject the standard `JobRepository` into your logic, and build the `Job` entity manually setting the `.setRunAt(OffsetDateTime)` property. JobQ will simply ignore the job until the system clock passes the scheduled run time!

### Using the HTMX Dashboard
JobQ ships with an embedded, lightning-fast dashboard that provides realtime insights into your queues. 

1. Start your Spring Boot application.
2. Navigate to `http://localhost:8080/jobq/dashboard`.
3. You'll see an heavily styled, dark-themed UI that streams live job updates atomically via HTMX `hx-trigger` attributes. No complex Javascript required!
   
**Securing the Dashboard**
By default, the dashboard is open. To secure it, simply add the following properties:
```yaml
jobq.dashboard.username=admin
jobq.dashboard.password=secure123
```
This enables an isolated HTTP Basic Auth Interceptor solely for `/jobq/**` paths, keeping your dashboard locked down without relying on immense overheads of heavy security architectures. 

### Micrometer Observability & Statistics
If `io.micrometer:micrometer-core` is present on your classpath (e.g. via `spring-boot-starter-actuator`), JobQ explicitly registers Native Gauges seamlessly using Spring Boot's internal `MeterRegistry`.
* `jobq.jobs.total`: The total count of jobs in the cluster.
* `jobq.jobs.count` (Tag: `status=PENDING|PROCESSING|COMPLETED|FAILED`): The exact count of jobs currently resting in each respective state.

These metrics auto-refresh continuously and plug perfectly into your existing Grafana or Prometheus dashboards.

---

## 🧹 Housekeeping
JobQ operates a discrete background sweeper task using standard Spring `@Scheduled` functions. It will automatically sweep and purge:
* Successfully processed (`COMPLETED`) jobs after `36h`. 
* Permanently `FAILED` jobs after `72h`.

No bloating, no maintenance, zero headaches.
