package com.spring_test.spring_batch.Controller;

import com.spring_test.spring_batch.model.Customer;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class CustomerController {

    private final JdbcTemplate jdbcTemplate;

    public CustomerController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/customers")
    public List<Customer> getCustomers() {
        return jdbcTemplate.query("SELECT * FROM customer", new BeanPropertyRowMapper<>(Customer.class));
    }
}
