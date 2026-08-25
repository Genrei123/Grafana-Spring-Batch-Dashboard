package com.spring_test.spring_batch.config;

import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring Batch 6's own default (DefaultBatchConfiguration.jobRepository(),
 * which Boot's autoconfiguration otherwise uses as-is) is
 * ResourcelessJobRepository - purely in-memory, no BATCH_JOB_INSTANCE/
 * BATCH_JOB_EXECUTION/... tables ever touched. That's fine for a job that
 * only needs to report its own outcome, but defeats the point here: Grafana
 * needs to query real rows in BATCH_JOB_EXECUTION. Extending
 * DefaultBatchConfiguration and overriding jobRepository() (Boot's own
 * default config backs off via @ConditionalOnMissingBean(DefaultBatchConfiguration.class)
 * once a subclass like this exists) swaps in a JDBC-backed one against our
 * real datasource instead. JobRepository extends JobExplorer in Spring Batch
 * 6, so this single bean also satisfies ImportJobHealthIndicator's
 * JobExplorer dependency - no separate bean needed.
 */
@Configuration
public class BatchRepositoryConfig extends DefaultBatchConfiguration {

    private final DataSource dataSource;
    private final PlatformTransactionManager transactionManager;

    public BatchRepositoryConfig(DataSource dataSource, PlatformTransactionManager transactionManager) {
        this.dataSource = dataSource;
        this.transactionManager = transactionManager;
    }

    @Override
    public JobRepository jobRepository() {
        try {
            JdbcJobRepositoryFactoryBean factory = new JdbcJobRepositoryFactoryBean();
            factory.setDataSource(dataSource);
            factory.setTransactionManager(transactionManager);
            factory.afterPropertiesSet();
            return factory.getObject();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to configure the JDBC-backed JobRepository", e);
        }
    }

    @Override
    protected PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }
}
