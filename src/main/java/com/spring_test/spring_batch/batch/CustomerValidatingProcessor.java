package com.spring_test.spring_batch.batch;

import com.spring_test.spring_batch.model.Customer;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Step-scoped so {@code simulateFailure} is re-read from each run's
 * JobParameters instead of being fixed once at application startup like an
 * ordinary singleton bean would be - see BatchJobController and
 * StartupJobRunner, which both set it explicitly on every launch.
 */
@Component
@StepScope
public class CustomerValidatingProcessor implements ItemProcessor<Customer, Customer> {

    private final boolean simulateFailure;

    public CustomerValidatingProcessor(@Value("#{jobParameters['simulateFailure']}") String simulateFailure) {
        this.simulateFailure = Boolean.parseBoolean(simulateFailure);
    }

    @Override
    public Customer process(Customer customer) {
        // Stands in for a real validation rule (e.g. a malformed row from an
        // upstream feed). Customer id=2 is the trigger so the demo is
        // reproducible: same file, same broken row, controlled entirely by
        // the simulateFailure flag.
        if (simulateFailure && customer.getId() == 2) {
            throw new IllegalStateException("Simulated failure for customer id=2 (simulateFailure=true)");
        }
        return customer;
    }
}
