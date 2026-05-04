package com.enigio.orchestrator.si.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "integration.retry")
public class IntegrationRetryProperties {

    private int maxAttempts = 4;
    private long initialIntervalMs = 2000;
    private double multiplier = 2.0;
    private long maxIntervalMs = 30000;
    private double jitterFactor = 0.5;
}
