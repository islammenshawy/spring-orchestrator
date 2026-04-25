package com.orchestrator.starter.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the orchestrator library.
 * Only properties the library needs. Everything else — concurrency,
 * ack mode, rebalancing, static membership, rack awareness, topic
 * creation — use standard Spring Kafka properties in application.yml.
 */
@Data
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

    private KafkaConfig kafka = new KafkaConfig();
    private RetryConfig retry = new RetryConfig();
    private RecoveryConfig recovery = new RecoveryConfig();
    private EndpointsConfig endpoints = new EndpointsConfig();

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

    @Data
    public static class EndpointsConfig {
        /** Auto-expose REST endpoints for flows */
        private boolean enabled = true;
        /** Base path for flow endpoints */
        private String basePath = "/flows";
    }
}
