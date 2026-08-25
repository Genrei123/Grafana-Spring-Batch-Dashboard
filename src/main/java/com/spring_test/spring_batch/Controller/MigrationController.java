package com.spring_test.spring_batch.Controller;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Two fixed-outcome endpoints for the ~15s migration POC demo (see
 * MigrationTasklet/app.migration.duration-ms): each launches migrationJob and
 * blocks until it finishes, since Boot's default JobLauncher runs
 * synchronously - the response only comes back once the job has completed or
 * failed.
 */
@RestController
public class MigrationController {

    private final JobLauncher jobLauncher;
    private final Job migrationJob;

    public MigrationController(JobLauncher jobLauncher, Job migrationJob) {
        this.jobLauncher = jobLauncher;
        this.migrationJob = migrationJob;
    }

    @PostMapping("/jobs/migrate/success")
    public ResponseEntity<String> migrateSuccess() throws Exception {
        return ResponseEntity.ok(run(false));
    }

    @PostMapping("/jobs/migrate/failure")
    public ResponseEntity<String> migrateFailure() throws Exception {
        return ResponseEntity.ok(run(true));
    }

    private String run(boolean simulateFailure) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("simulateFailure", String.valueOf(simulateFailure))
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(migrationJob, params);

        return "executionId=" + execution.getId()
                + " status=" + execution.getStatus()
                + " exitCode=" + execution.getExitStatus().getExitCode();
    }
}
