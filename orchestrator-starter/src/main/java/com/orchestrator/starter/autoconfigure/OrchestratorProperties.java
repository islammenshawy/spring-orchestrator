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
    private RetentionConfig retention = new RetentionConfig();
    private AuditConfig audit = new AuditConfig();

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
        private boolean enabled = true;
        private String basePath = "/flows";
    }

    @Data
    public static class RetentionConfig {
        /** Auto-delete published outbox events after N days (TTL index) */
        private int outboxDays = 7;
        /** Auto-delete idempotency records after N days (TTL index) */
        private int processedEventsDays = 30;
        /** Auto-delete step audit logs after N days (TTL index). 0 = keep forever. */
        private int stepLogDays = 90;
    }

    @Data
    public static class AuditConfig {
        /** Include full flow state JSON (before/after) in step logs. Disable for performance at scale. */
        private boolean includeFlowState = false;
    }
}
