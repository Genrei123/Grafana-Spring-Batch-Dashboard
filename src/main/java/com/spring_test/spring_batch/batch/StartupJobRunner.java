package com.spring_test.spring_batch.batch;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Launches importCustomerJob once at startup, entirely config-driven: flip
 * app.batch.simulate-failure in application.yaml and restart to see the job
 * succeed or fail on boot. Boot's own auto-launcher is disabled
 * (spring.batch.job.enabled=false) so this is the only thing that runs it at
 * startup - BatchJobController below can also (re-)launch it on demand
 * without restarting the app.
 */
@Component
public class StartupJobRunner implements ApplicationRunner {

    private final JobLauncher jobLauncher;
    private final Job importCustomerJob;
    private final boolean runOnStartup;
    private final boolean simulateFailure;

    public StartupJobRunner(JobLauncher jobLauncher,
                             Job importCustomerJob,
                             @Value("${app.batch.run-on-startup:true}") boolean runOnStartup,
                             @Value("${app.batch.simulate-failure:false}") boolean simulateFailure) {
        this.jobLauncher = jobLauncher;
        this.importCustomerJob = importCustomerJob;
        this.runOnStartup = runOnStartup;
        this.simulateFailure = simulateFailure;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!runOnStartup) {
            return;
        }
        JobParameters params = new JobParametersBuilder()
                .addString("simulateFailure", String.valueOf(simulateFailure))
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(importCustomerJob, params);
    }
}
