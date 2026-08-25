package com.spring_test.spring_batch.batch;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Bridges a Spring Batch job's outcome into the two other observability
 * signals: a Micrometer counter (so /actuator/metrics and Prometheus can
 * alert on a rising failure rate) and an OpenTelemetry span (so a single
 * failed run can be inspected end-to-end in a trace viewer, with the actual
 * exception attached). The custom health indicator (ImportJobHealthIndicator)
 * covers the third angle: "is the app healthy right now."
 */
@Component
public class CustomerImportJobListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(CustomerImportJobListener.class);

    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    // One job runs at a time in this demo, so instance fields are fine; a
    // ThreadLocal would be needed if jobs could run concurrently on this listener.
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
        jobSpan.tag("batch.job.execution.id", String.valueOf(jobExecution.getId()));
        log.info("Job [{}] starting (executionId={})", jobName, jobExecution.getId());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        BatchStatus status = jobExecution.getStatus();
        Duration duration = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime());

        // Counter tagged by job name + outcome - this is what you'd graph in
        // Grafana and alert on (e.g. "rate of status=FAILED > 0 over 5m").
        // Tag key is "job_name", not "job": Prometheus's scrape config already
        // attaches a label called "job" (the scrape job's own name,
        // "spring-batch") to every metric from this target. A same-named
        // custom tag collides with it and - since honor_labels isn't set -
        // Prometheus's own value wins, silently renaming ours to
        // "exported_job" instead. Panels filtering on job="importCustomerJob"
        // would then never match anything.
        meterRegistry.counter("batch.job.executions", "job_name", jobName, "status", status.name()).increment();

        jobSpan.tag("batch.job.status", status.name());
        jobSpan.tag("batch.job.duration.ms", String.valueOf(duration.toMillis()));

        if (status == BatchStatus.FAILED) {
            List<Throwable> failures = jobExecution.getAllFailureExceptions();
            Throwable cause = failures.isEmpty() ? null : failures.get(0);
            log.error("Job [{}] FAILED after {} ms (executionId={})",
                    jobName, duration.toMillis(), jobExecution.getId(), cause);
            if (cause != null) {
                jobSpan.error(cause); // marks the span as an error trace in the tracing backend
            }
        } else {
            log.info("Job [{}] {} after {} ms (executionId={})",
                    jobName, status, duration.toMillis(), jobExecution.getId());
        }

        jobSpan.end();
        spanScope.close();
    }
}
