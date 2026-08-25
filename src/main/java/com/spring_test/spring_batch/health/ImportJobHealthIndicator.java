package com.spring_test.spring_batch.health;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the app as DOWN whenever the customer-import job's most recent run
 * failed. Bean name minus the "HealthIndicator" suffix becomes the component
 * key under /actuator/health, i.e. this shows up as "importJob".
 *
 * This is the same pattern production teams use to fold "did my critical
 * batch job succeed?" into infrastructure that already watches
 * /actuator/health (Kubernetes liveness/readiness probes, load balancer
 * health checks, uptime monitors) instead of needing a separate check.
 */
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
        if (lastInstance == null) {
            return Health.unknown().withDetail("message", "no runs yet").build();
        }

        JobExecution lastExecution = jobExplorer.getLastJobExecution(lastInstance);
        if (lastExecution == null) {
            return Health.unknown().withDetail("message", "instance found but no execution recorded").build();
        }

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
