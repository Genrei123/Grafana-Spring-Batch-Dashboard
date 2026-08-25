package com.spring_test.spring_batch.config;

import com.spring_test.spring_batch.batch.CustomerImportJobListener;
import com.spring_test.spring_batch.model.Customer;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Reads a customer CSV (path config-driven via app.batch.customers-file) and
 * writes each row into the "customer" table in the embedded H2 database. The
 * chunk size (app.batch.chunk-size) is small on purpose so a single failure
 * fails the whole chunk's transaction - see CustomerValidatingProcessor.
 */
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
        // time this job is (re-)launched for the demo, and a plain INSERT would
        // hit a primary-key violation on the second successful run - especially
        // now that the real datasource is a persistent SQL Server rather than
        // H2 resetting on every restart. Standard SQL:2003 MERGE syntax (as
        // opposed to H2's own shorthand "MERGE INTO t (...) KEY (...) VALUES
        // (...)", which SQL Server doesn't understand) so this works unchanged
        // against both H2 (tests) and SQL Server (real run).
        return new JdbcBatchItemWriterBuilder<Customer>()
                .dataSource(dataSource)
                .sql("""
                        MERGE INTO customer AS target
                        USING (VALUES (:id, :name, :email)) AS source (id, name, email)
                        ON target.id = source.id
                        WHEN MATCHED THEN UPDATE SET name = source.name, email = source.email
                        WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (source.id, source.name, source.email);
                        """)
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
                .<Customer, Customer>chunk(chunkSize, transactionManager)
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
                .listener(customerImportJobListener)
                .build();
    }
}
