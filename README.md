# spring-batch: a Spring Batch + Actuator + OpenTelemetry walkthrough

A small, from-scratch example of a Spring Boot app that:

- runs a Spring Batch job (`importCustomerJob`) that reads a CSV and writes it to a database,
- exposes [Spring Boot Actuator](https://docs.spring.io/spring-boot/reference/actuator/index.html) endpoints to inspect the running instance (health, metrics, env, ...),
- emits [OpenTelemetry](https://opentelemetry.io/) traces for every job run, and
- can be told, purely through config, to fail on demand - so you can see exactly what a failed batch job looks like across health checks, metrics, logs, and traces.

This document rebuilds the project step by step, starting from Spring Initializr and ending with deliberately breaking the job to see the failure show up everywhere it should.

**Stack**: Java 17, Spring Boot 4.1.0, Spring Batch 6, Maven. Uses an in-memory H2 database, so nothing external (no Docker, no real DB) is required to follow along.

---

## Prerequisites

- JDK 17+
- No local Maven needed - use the wrapper (`./mvnw` / `mvnw.cmd`) checked into the repo
- (Optional, for the tracing section) [Docker](https://www.docker.com/), if you want to view real traces in Jaeger instead of console output

---

## Step 1 - Start from Spring Initializr

Go to [start.spring.io](https://start.spring.io) and generate a project with:

| Field | Value |
|---|---|
| Project | Maven |
| Language | Java |
| Spring Boot | 4.1.x |
| Group | `com.spring-test` |
| Artifact | `spring-batch` |
| Packaging | Jar |
| Java | 17 |
| Dependencies | **Spring Web** |

That's it for the initial generation - everything else (Batch, Actuator, OpenTelemetry) gets added by hand in the steps below, so you can see exactly what each one contributes. Download and unzip it, and you'll get:

```
pom.xml
mvnw / mvnw.cmd
src/main/java/.../SpringBatchApplication.java   # @SpringBootApplication + main()
src/main/resources/application.yaml
src/test/java/.../SpringBatchApplicationTests.java
```

Add a trivial controller so there's something to hit immediately:

```java
// src/main/java/com/spring_test/spring_batch/Controller/HelloController.java
@RestController
public class HelloController {
    @GetMapping("/hello")
    public String hello() {
        return "Hello, World!";
    }
}
```

Run it and confirm the baseline works before adding anything else:

```
mvnw.cmd spring-boot:run
curl http://localhost:8080/hello   # -> Hello, World!
```

---

## Step 2 - Add the Spring Batch job (the "simple job migration")

### 2.1 Dependencies

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-batch</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-jdbc</artifactId>
</dependency>

<!-- Embedded, in-memory DB. Boot auto-configures it with zero config (no
     spring.datasource.url needed) and auto-creates both Spring Batch's own
     metadata schema and our schema.sql on startup. Resets on every restart -
     fine for learning; swap for a real driver + explicit datasource config
     in production. -->
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>
```

### 2.2 The schema and the source data

```sql
-- src/main/resources/schema.sql
-- Runs automatically on startup against the embedded H2 DB (Boot's default
-- spring.sql.init.mode=embedded). Spring Batch's own BATCH_JOB_INSTANCE /
-- BATCH_STEP_EXECUTION / ... tables are created separately and automatically
-- by spring-boot-starter-batch for the same reason - no config needed there.
CREATE TABLE IF NOT EXISTS customer (
    id    BIGINT PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);
```

```csv
# src/main/resources/data/customers.csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Carol,carol@example.com
```

### 2.3 The model

```java
public class Customer {
    private Long id;
    private String name;
    private String email;
    // getters/setters
}
```

### 2.4 The job: reader -> processor -> writer

The processor is the interesting part - it's what lets us simulate a failure **without touching code**, by reading a value out of the job's own parameters:

```java
// batch/CustomerValidatingProcessor.java
@Component
@StepScope // re-resolved per run from JobParameters, not fixed at startup like a normal singleton
public class CustomerValidatingProcessor implements ItemProcessor<Customer, Customer> {

    private final boolean simulateFailure;

    public CustomerValidatingProcessor(@Value("#{jobParameters['simulateFailure']}") String simulateFailure) {
        this.simulateFailure = Boolean.parseBoolean(simulateFailure);
    }

    @Override
    public Customer process(Customer customer) {
        if (simulateFailure && customer.getId() == 2) {
            throw new IllegalStateException("Simulated failure for customer id=2 (simulateFailure=true)");
        }
        return customer;
    }
}
```

```java
// config/CustomerImportJobConfig.java
@Configuration
public class CustomerImportJobConfig {

    @Value("${app.batch.customers-file:classpath:data/customers.csv}")
    private String customersFile;

    @Value("${app.batch.chunk-size:10}")
    private int chunkSize;

    @Bean
    public ItemReader<Customer> customerItemReader(ResourceLoader resourceLoader) {
        Resource resource = resourceLoader.getResource(customersFile);
        return new FlatFileItemReaderBuilder<Customer>()
                .name("customerItemReader")
                .resource(resource)
                .linesToSkip(1)
                .delimited()
                .names("id", "name", "email")
                .targetType(Customer.class)
                .build();
    }

    @Bean
    public ItemWriter<Customer> customerItemWriter(DataSource dataSource) {
        // MERGE (upsert), not INSERT: the same static CSV gets re-imported every
        // time this job runs during the demo, and a plain INSERT would hit a
        // primary-key violation on the second successful run. A real pipeline
        // pulling a fresh file each run wouldn't need this.
        return new JdbcBatchItemWriterBuilder<Customer>()
                .dataSource(dataSource)
                .sql("MERGE INTO customer (id, name, email) KEY (id) VALUES (:id, :name, :email)")
                .beanMapped()
                .build();
    }

    @Bean
    public Step importCustomerStep(JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager,
                                    ItemReader<Customer> customerItemReader,
                                    ItemProcessor<Customer, Customer> customerValidatingProcessor,
                                    ItemWriter<Customer> customerItemWriter) {
        return new StepBuilder("importCustomerStep", jobRepository)
                .<Customer, Customer>chunk(chunkSize, transactionManager) // chunk = one transaction
                .reader(customerItemReader)
                .processor(customerValidatingProcessor)
                .writer(customerItemWriter)
                .build();
    }

    @Bean
    public Job importCustomerJob(JobRepository jobRepository, Step importCustomerStep,
                                  CustomerImportJobListener customerImportJobListener) {
        return new JobBuilder("importCustomerJob", jobRepository)
                .start(importCustomerStep)
                .listener(customerImportJobListener) // easy to forget - the listener does nothing without this
                .build();
    }
}
```

Chunk size 10 with only 3 rows means all of them commit as **one transaction** - if row 2 fails, nothing from that run is persisted. That matters later when we simulate a failure: you'll see the table is untouched, not half-written.

### 2.5 Launching it

Two ways to launch `importCustomerJob`, both building `JobParameters` explicitly (so `simulateFailure` is always present for the processor above to read):

```java
// batch/StartupJobRunner.java - runs once automatically at boot
@Component
public class StartupJobRunner implements ApplicationRunner {
    private final JobLauncher jobLauncher;
    private final Job importCustomerJob;
    private final boolean runOnStartup;
    private final boolean simulateFailure;

    public StartupJobRunner(JobLauncher jobLauncher, Job importCustomerJob,
                             @Value("${app.batch.run-on-startup:true}") boolean runOnStartup,
                             @Value("${app.batch.simulate-failure:false}") boolean simulateFailure) {
        this.jobLauncher = jobLauncher;
        this.importCustomerJob = importCustomerJob;
        this.runOnStartup = runOnStartup;
        this.simulateFailure = simulateFailure;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!runOnStartup) return;
        JobParameters params = new JobParametersBuilder()
                .addString("simulateFailure", String.valueOf(simulateFailure))
                .addLong("run.id", System.currentTimeMillis()) // makes each launch a distinct JobInstance
                .toJobParameters();
        jobLauncher.run(importCustomerJob, params);
    }
}
```

```java
// Controller/BatchJobController.java - re-launch on demand, no restart needed
@RestController
public class BatchJobController {
    private final JobLauncher jobLauncher;
    private final Job importCustomerJob;
    private final boolean defaultSimulateFailure;

    public BatchJobController(JobLauncher jobLauncher, Job importCustomerJob,
                               @Value("${app.batch.simulate-failure:false}") boolean defaultSimulateFailure) {
        this.jobLauncher = jobLauncher;
        this.importCustomerJob = importCustomerJob;
        this.defaultSimulateFailure = defaultSimulateFailure;
    }

    @PostMapping("/jobs/import-customers")
    public ResponseEntity<String> launch(@RequestParam(required = false) Boolean simulateFailure) throws Exception {
        boolean flag = simulateFailure != null ? simulateFailure : defaultSimulateFailure;
        JobParameters params = new JobParametersBuilder()
                .addString("simulateFailure", String.valueOf(flag))
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();
        JobExecution execution = jobLauncher.run(importCustomerJob, params);
        return ResponseEntity.ok("executionId=" + execution.getId()
                + " status=" + execution.getStatus()
                + " exitCode=" + execution.getExitStatus().getExitCode());
    }
}
```

Boot's own auto-launcher is turned off in `application.yaml` (`spring.batch.job.enabled: false`) so `StartupJobRunner` is the only thing launching the job at boot - otherwise the job would run twice, once with Boot's default (empty) parameters and once with ours.

```yaml
spring:
  batch:
    job:
      enabled: false   # StartupJobRunner launches it instead, with explicit params
    jdbc:
      initialize-schema: embedded

app:
  batch:
    simulate-failure: false                    # <- the whole demo hinges on this one line
    run-on-startup: true
    customers-file: classpath:data/customers.csv
    chunk-size: 10
```

At this point `mvnw.cmd spring-boot:run` boots, imports 3 customers, and a `CustomerController` (`GET /customers`) lets you check the table via `JdbcTemplate`.

---

## Step 3 - Add Spring Boot Actuator

### 3.1 Dependency

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

### 3.2 Configuration, endpoint by endpoint

```yaml
management:
  endpoints:
    web:
      exposure:
        # Only "health" and "info" are exposed over HTTP by default - everything
        # else (env, beans, metrics, mappings, ...) can leak internals, so it
        # must be opted into explicitly. Listed individually here so it's
        # obvious what's turned on ("*" would expose everything at once).
        include: health,info,metrics,env,beans,mappings,loggers,threaddump,heapdump,prometheus

  endpoint:
    health:
      # never (default) = just {"status":"UP"}; when-authorized = full detail
      # but only to an authenticated caller; always = full breakdown always.
      # "always" is fine for local learning - tighten this once reachable
      # outside your machine, since it can reveal internal details.
      show-details: always
    shutdown:
      # /actuator/shutdown lets a POST request kill the app. Off on purpose -
      # obviously dangerous on anything network-reachable.
      enabled: false

  info:
    env:
      enabled: true   # let values under the top-level "info:" block show up at /actuator/info
    java:
      enabled: true   # add JVM vendor/version to /actuator/info
    os:
      enabled: true    # add OS name/arch/version to /actuator/info

info:
  app:
    name: ${spring.application.name}
    description: Spring Batch + Actuator + OpenTelemetry demo
```

| Endpoint | Shows | Notes |
|---|---|---|
| `/actuator/health` | UP/DOWN + per-component breakdown | Feeds Kubernetes liveness/readiness probes and load balancer health checks |
| `/actuator/info` | Whatever's under `info:`, plus JVM/OS facts | Static/build metadata |
| `/actuator/metrics`, `/actuator/prometheus` | Counters/gauges/timers | The Prometheus-format one needs `micrometer-registry-prometheus` (added in the tracing step below) |
| `/actuator/env` | Every resolved config property | **Never expose this publicly** - can leak secrets |
| `/actuator/beans`, `/mappings` | Full context / all routes | Debugging aid, high information-disclosure risk if left open |

### 3.3 A custom health indicator for the job

This is the piece that turns "did my batch job succeed?" into something your infrastructure already watches:

```java
// health/ImportJobHealthIndicator.java
// Bean name minus the "HealthIndicator" suffix becomes the /actuator/health
// component key - this shows up as "importJob".
@Component("importJob")
public class ImportJobHealthIndicator implements HealthIndicator {
    private static final String JOB_NAME = "importCustomerJob";
    private final JobExplorer jobExplorer;

    public ImportJobHealthIndicator(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    @Override
    public Health health() {
        JobInstance lastInstance = jobExplorer.getLastJobInstance(JOB_NAME);
        if (lastInstance == null) return Health.unknown().withDetail("message", "no runs yet").build();

        JobExecution lastExecution = jobExplorer.getLastJobExecution(lastInstance);
        Health.Builder builder = switch (lastExecution.getStatus()) {
            case COMPLETED -> Health.up();
            case FAILED -> Health.down();
            default -> Health.unknown();
        };
        return builder
                .withDetail("jobName", JOB_NAME)
                .withDetail("status", lastExecution.getStatus())
                .withDetail("exitCode", lastExecution.getExitStatus().getExitCode())
                .withDetail("startTime", lastExecution.getStartTime())
                .withDetail("endTime", lastExecution.getEndTime())
                .build();
    }
}
```

Boot maps overall `/actuator/health` status `DOWN` to **HTTP 503** by default - so one failed job execution is enough to make load balancers stop routing traffic to this instance, without writing any HTTP-layer code for it.

---

## Step 4 - Add OpenTelemetry (OTel)

Actuator tells you about **one running instance right now**. OTel tells you what happened **during one specific run**, step by step, correlated across every hop by a trace ID - that's the piece that lets you look at a single failed job execution and see exactly where and why it broke.

### 4.1 Dependency

Boot 4.1 ships a single starter that bundles the Micrometer→OTel tracing bridge, the OTLP exporter, *and* - critically - the Boot autoconfiguration modules that actually register the `Tracer` bean your code injects. Wiring the raw `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp` jars in by hand is **not** enough on this Boot version; those autoconfiguration classes live in Boot's own `spring-boot-opentelemetry` / `spring-boot-micrometer-tracing-opentelemetry` modules, which only the starter pulls in.

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-opentelemetry</artifactId>
</dependency>

<!-- Prints every finished span to the console too - useful when you don't have
     a collector running yet. Safe to drop once always pointed at a real backend. -->
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-logging</artifactId>
</dependency>
```

```java
// config/TracingConfig.java
// Registering a SpanExporter bean adds it alongside the OTLP exporter Boot
// already configures from management.otlp.tracing.endpoint (both run at once).
@Configuration
public class TracingConfig {
    @Bean
    public SpanExporter loggingSpanExporter() {
        return LoggingSpanExporter.create();
    }
}
```

### 4.2 Configuration

```yaml
management:
  tracing:
    sampling:
      probability: 1.0   # trace every run locally; lower this in prod to cut volume/cost
  otlp:
    tracing:
      # OTLP/HTTP port a real collector listens on. With nothing listening
      # there, this exporter just logs connection warnings and drops spans -
      # harmless. Point it at a real one when you have it, e.g.:
      #   docker run -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/all-in-one:latest
      # then view traces at http://localhost:16686.
      endpoint: http://localhost:4318/v1/traces
```

### 4.3 Recording a span per job execution

```java
// batch/CustomerImportJobListener.java
@Component
public class CustomerImportJobListener implements JobExecutionListener {
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private Span jobSpan;
    private Tracer.SpanInScope spanScope;

    public CustomerImportJobListener(MeterRegistry meterRegistry, Tracer tracer) {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        jobSpan = tracer.nextSpan().name("batch.job." + jobName).start();
        spanScope = tracer.withSpan(jobSpan);
        jobSpan.tag("batch.job.name", jobName);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        BatchStatus status = jobExecution.getStatus();

        // The counter you'd graph/alert on: rate of status=FAILED > 0 over 5m.
        meterRegistry.counter("batch.job.executions",
                "job", jobExecution.getJobInstance().getJobName(),
                "status", status.name()).increment();

        jobSpan.tag("batch.job.status", status.name());
        if (status == BatchStatus.FAILED) {
            List<Throwable> failures = jobExecution.getAllFailureExceptions();
            if (!failures.isEmpty()) jobSpan.error(failures.get(0)); // marks the span as an error in the trace backend
        }
        jobSpan.end();
        spanScope.close();
    }
}
```

Two things worth knowing before you assume this is the only source of tracing data:

- **Spring Batch 6 auto-instruments jobs and steps on its own** once Micrometer Tracing is on the classpath - you'll see `spring.batch.job` and `spring.batch.step` spans in the console even without writing `CustomerImportJobListener` yourself. The listener above adds a span with our own tags and, more importantly, the Micrometer **counter** that Batch doesn't create for you.
- All spans from one job run - Batch's own plus this one plus (if triggered over HTTP) the request span - share the same **trace ID**, so a tracing UI shows them as one connected waterfall.

Also add the Prometheus registry so `/actuator/prometheus` has something to serve:

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

---

## Step 5 - Run it (the happy path)

```
mvnw.cmd spring-boot:run
```

```
Job [importCustomerJob] starting (executionId=1)
Job [importCustomerJob] COMPLETED after 78 ms (executionId=1)
```

```
curl http://localhost:8080/customers
# -> [{"id":1,"name":"Alice",...}, {"id":2,"name":"Bob",...}, {"id":3,"name":"Carol",...}]

curl http://localhost:8080/actuator/health
# -> {"status":"UP", "components": {"importJob": {"status":"UP", "details": {"status":"COMPLETED", ...}}, ...}}
```

---

## Step 6 - Simulate the failure and watch it show up everywhere

This is the part the whole `simulateFailure` mechanism exists for. Trigger a run with the flag flipped, **without restarting the app**:

```
curl -X POST "http://localhost:8080/jobs/import-customers?simulateFailure=true"
```

```
executionId=1 status=FAILED exitCode=FAILED
```

Now check each observability signal in turn:

**1. Logs** - the exact exception, attached to the job:
```
java.lang.IllegalStateException: Simulated failure for customer id=2 (simulateFailure=true)
Job [importCustomerJob] FAILED after 14 ms (executionId=1)
```

**2. Health** - `GET /actuator/health` now returns **HTTP 503**:
```json
{
  "status": "DOWN",
  "components": {
    "importJob": {
      "status": "DOWN",
      "details": { "jobName": "importCustomerJob", "status": "FAILED", "exitCode": "FAILED", ... }
    },
    "db": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```
Note only the `importJob` component is down - the rest of the app is fine, which is exactly the resolution you want: "this one job is broken," not "the whole instance is unhealthy."

**3. Metrics** - `GET /actuator/prometheus`:
```
batch_job_executions_total{job="importCustomerJob",status="COMPLETED"} 1.0
batch_job_executions_total{job="importCustomerJob",status="FAILED"} 1.0
```
This is what a Prometheus alert rule would fire on, e.g. `rate(batch_job_executions_total{status="FAILED"}[5m]) > 0`.

**4. Traces** - the console (or Jaeger, if you pointed the OTLP exporter at one) shows one trace ID tying it together:
```
'spring.batch.step' : <traceId> ... {spring.batch.step.status="FAILED", ...}
'spring.batch.job'  : <traceId> ... {spring.batch.job.status="FAILED", ...}
'batch.job.importCustomerJob' : <traceId> ... {batch.job.status="FAILED"}   <- our span, exception attached
```

**5. Data integrity** - because the whole 3-row CSV is one chunk/transaction, the failure on row 2 rolled back the *entire* run:
```
curl http://localhost:8080/customers
# -> still exactly Alice, Bob, Carol from the earlier successful run - nothing corrupted
```

### Recovering

Flip it back, no restart required:

```
curl -X POST "http://localhost:8080/jobs/import-customers?simulateFailure=false"
# -> executionId=1 status=COMPLETED exitCode=COMPLETED

curl http://localhost:8080/actuator/health
# -> HTTP 200, status UP, importJob UP again
```

The **permanent** default lives in `application.yaml` (`app.batch.simulate-failure: false`); the `?simulateFailure=` query parameter above only overrides it for that one ad-hoc call, which is what makes this whole walkthrough repeatable without restarting the app between steps.

---

## Step 7 - Graph it in Grafana

`/actuator/prometheus` is just text until something scrapes and graphs it. `docker-compose.yml` at the repo root brings up Prometheus (scrapes the app every 5s) and Grafana (pre-wired to Prometheus, with a dashboard already provisioned) - no manual datasource or dashboard setup needed.

```
mvnw.cmd spring-boot:run        # app must be running on :8080 first
docker compose up -d            # starts Prometheus (:9090) and Grafana (:3000)
```

Open **http://localhost:3000** - anonymous access is on for this local demo, so it goes straight to **Spring Batch - Customer Import**, no login needed (or sign in as `admin` / `admin` for the default Grafana admin account). It shows:

- **App health** row - up/down, uptime, COMPLETED vs FAILED run counts, JVM heap gauge
- **Batch job** row - job executions by status (from `batch.job.executions`/`CustomerImportJobListener`) and the failed-run rate you'd alert on
- **HTTP** row - request rate by route and p95 latency, from Micrometer's auto-instrumented `http.server.requests`
- **JVM / process** row - heap used and CPU usage over time

Re-run the failure demo from Step 6 while watching it - `POST /jobs/import-customers?simulateFailure=true` shows up as a red bar in "Job executions by status" and a bump in "Job runs - FAILED" within one 5s scrape interval.

```
docker compose down             # stop Prometheus + Grafana (dashboard/config persist on disk, no volumes to lose)
```

| File | Role |
|---|---|
| `docker-compose.yml` | Prometheus + Grafana services |
| `monitoring/prometheus/prometheus.yml` | Scrapes `host.docker.internal:8080/actuator/prometheus` every 5s |
| `monitoring/grafana/provisioning/datasources/datasource.yml` | Auto-registers the Prometheus datasource |
| `monitoring/grafana/provisioning/dashboards/dashboards.yml` | Tells Grafana to load dashboards from `monitoring/grafana/dashboards/` |
| `monitoring/grafana/dashboards/spring-batch.json` | The dashboard itself - edit and reload (`updateIntervalSeconds: 10`) to iterate |

---

## Reference: file map

| File | Role |
|---|---|
| `pom.xml` | `spring-boot-starter-web`, `-actuator`, `-batch`, `-jdbc`, `-opentelemetry`, plus `h2`, `opentelemetry-exporter-logging`, `micrometer-registry-prometheus` |
| `src/main/resources/application.yaml` | All config: batch launch behavior, `app.batch.*` demo knobs, Actuator endpoint exposure, tracing/OTLP |
| `src/main/resources/schema.sql`, `data/customers.csv` | H2 table DDL + fixture data |
| `model/Customer.java` | Row shape |
| `config/CustomerImportJobConfig.java` | Reader/processor/writer/step/job wiring |
| `batch/CustomerValidatingProcessor.java` | The config-driven failure trigger |
| `batch/CustomerImportJobListener.java` | Metrics counter + OTel span per job execution |
| `batch/StartupJobRunner.java` | Launches the job once at boot |
| `health/ImportJobHealthIndicator.java` | Folds job status into `/actuator/health` |
| `config/TracingConfig.java` | Console span exporter for local viewing |
| `Controller/BatchJobController.java` | `POST /jobs/import-customers` - relaunch on demand |
| `Controller/CustomerController.java` | `GET /customers` |
| `Controller/HelloController.java` | `GET /hello` |

## Gotchas hit while building this

- **`src/test/resources/application.yaml` replaces `src/main/resources/application.yaml` entirely for tests**, rather than merging with it (test-classes precedes classes on the classpath, and Boot only loads the first `application.yaml` it finds at a given classpath location). Any property the test context needs - including `spring.batch.job.enabled: false` - has to be repeated there, or given a code-level default via `@Value("${...:default}")`.
- **Boot 4.1 restructured Spring Batch's and Actuator's packages** (e.g. `org.springframework.batch.core.job.Job`, `org.springframework.boot.health.contributor.Health` instead of the older `org.springframework.boot.actuate.health.Health`). If you're following older tutorials, expect import paths to differ.
- **A `JobExecutionListener` bean does nothing until attached with `.listener(...)` on the `JobBuilder`** - Spring Batch doesn't auto-discover and apply every listener bean to every job.
#   G r a f a n a - S p r i n g - B a t c h - D a s h b o a r d  
 