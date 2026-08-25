package com.spring_test.spring_batch.Controller;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Re-launches importCustomerJob on demand, without an app restart - handy for
 * this walkthrough. The permanent, config-driven default lives in
 * application.yaml (app.batch.simulate-failure); the simulateFailure query
 * param here just overrides it for one ad-hoc run.
 */
@RestController
public class BatchJobController {

    private final JobLauncher jobLauncher;
    private final Job importCustomerJob;
    private final boolean defaultSimulateFailure;

    public BatchJobController(JobLauncher jobLauncher,
                               Job importCustomerJob,
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

        String body = "executionId=" + execution.getId()
                + " status=" + execution.getStatus()
                + " exitCode=" + execution.getExitStatus().getExitCode();
        return ResponseEntity.ok(body);
    }
}
