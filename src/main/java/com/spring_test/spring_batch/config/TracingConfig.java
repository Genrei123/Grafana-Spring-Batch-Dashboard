package com.spring_test.spring_batch.config;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registering a SpanExporter bean adds it alongside the OTLP exporter Boot
 * already configures from management.otlp.tracing.endpoint (both run at
 * once). This one just prints each finished span to the console, so spans
 * are visible without a collector running - remove once you're always
 * pointed at a real backend (Jaeger/Tempo/Datadog/...).
 */
@Configuration
public class TracingConfig {

    @Bean
    public SpanExporter loggingSpanExporter() {
        return LoggingSpanExporter.create();
    }
}
