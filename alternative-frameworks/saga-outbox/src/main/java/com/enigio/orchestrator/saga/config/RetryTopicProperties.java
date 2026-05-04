package com.enigio.orchestrator.saga.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.retry")
public class RetryTopicProperties {

    /** Total attempts including the initial one (e.g. 4 = 1 initial + 3 retries) */
    private int maxAttempts = 4;

    /** Initial backoff delay in milliseconds */
    private long initialIntervalMs = 2000;

    /** Multiplier for exponential backoff (delay * multiplier^attempt) */
    private double multiplier = 2.0;

    /** Maximum backoff delay cap in milliseconds */
    private long maxIntervalMs = 30000;

    /** Jitter factor: 0.0 = no jitter, 0.5 = equal jitter, 1.0 = full jitter */
    private double jitterFactor = 0.5;
}
