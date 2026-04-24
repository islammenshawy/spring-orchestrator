package com.orchestrator.starter.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the orchestrator starter.
 *
 * application.yml:
 * <pre>
 * orchestrator:
 *   kafka:
 *     command-topic: my-service.commands    # Kafka topic for step commands
 *   retry:
 *     max-attempts: 4                      # 1 initial + 3 retries
 *     initial-interval-ms: 2000            # first retry after 2s
 *     multiplier: 2.0                      # exponential: 2s → 4s → 8s
 *     max-interval-ms: 30000               # cap at 30s
 *     jitter-factor: 0.5                   # 0.0=none, 0.5=equal, 1.0=full
 *   recovery:
 *     scan-interval-ms: 30000              # check for stale flows every 30s
 *     stale-threshold-minutes: 5           # consider stale after 5 min
 * </pre>
 */
@Data
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

    private KafkaConfig kafka = new KafkaConfig();
    private RetryConfig retry = new RetryConfig();
    private RecoveryConfig recovery = new RecoveryConfig();

    @Data
    public static class KafkaConfig {
        private String commandTopic = "orchestrator.commands";
    }

    @Data
    public static class RetryConfig {
        private int maxAttempts = 4;
        private long initialIntervalMs = 2000;
        private double multiplier = 2.0;
        private long maxIntervalMs = 30000;
        private double jitterFactor = 0.5;
    }

    @Data
    public static class RecoveryConfig {
        private long scanIntervalMs = 30000;
        private int staleThresholdMinutes = 5;
    }
}
