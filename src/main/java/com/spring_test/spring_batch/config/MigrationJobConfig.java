package com.spring_test.spring_batch.config;

import com.spring_test.spring_batch.batch.CustomerImportJobListener;
import com.spring_test.spring_batch.batch.MigrationTasklet;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A second, independent job used purely to demo a longer-running (~15s)
 * migration in Grafana: MigrationTasklet sleeps for app.migration.duration-ms
 * then either finishes or throws, based on the simulateFailure job parameter
 * - see MigrationController for the two endpoints that launch it. Reuses
 * CustomerImportJobListener (it tags metrics/spans by job name already, so
 * nothing import-specific about it) to get the same Prometheus counter and
 * tracing coverage as importCustomerJob for free.
 */
@Configuration
public class MigrationJobConfig {

    @Bean
    public Step migrationStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager,
                               MigrationTasklet migrationTasklet) {
        return new StepBuilder("migrationStep", jobRepository)
                .tasklet(migrationTasklet, transactionManager)
                .build();
    }

    @Bean
    public Job migrationJob(JobRepository jobRepository, Step migrationStep,
                             CustomerImportJobListener customerImportJobListener) {
        return new JobBuilder("migrationJob", jobRepository)
                .start(migrationStep)
                .listener(customerImportJobListener)
                .build();
    }
}
