package com.spring_test.spring_batch.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Stands in for a real migration step: sleeps for app.migration.duration-ms
 * (default 15s) to give the Grafana panels something to watch, then either
 * finishes or throws depending on the simulateFailure job parameter - see
 * MigrationController, which sets that parameter explicitly on every launch.
 * Step-scoped for the same reason as CustomerValidatingProcessor: so
 * simulateFailure is re-read per run instead of fixed at startup.
 */
@Component
@StepScope
public class MigrationTasklet implements Tasklet {

    private final boolean simulateFailure;
    private final long durationMillis;

    public MigrationTasklet(@Value("#{jobParameters['simulateFailure']}") String simulateFailure,
                             @Value("${app.migration.duration-ms:15000}") long durationMillis) {
        this.simulateFailure = Boolean.parseBoolean(simulateFailure);
        this.durationMillis = durationMillis;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Thread.sleep(durationMillis);
        if (simulateFailure) {
            throw new IllegalStateException("Simulated migration failure after " + durationMillis + "ms");
        }
        return RepeatStatus.FINISHED;
    }
}
