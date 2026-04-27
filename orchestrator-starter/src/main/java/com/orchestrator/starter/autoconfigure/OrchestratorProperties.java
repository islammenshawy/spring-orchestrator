package com.orchestrator.starter.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

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

    /** Per-flow configuration overrides. Key = flowType name from @Flow(name="...").
     *  Only set fields override — null fields fall back to global config. */
    private Map<String, FlowConfig> flows = new LinkedHashMap<>();
    private AuditConfig audit = new AuditConfig();

    @Data
    public static class KafkaConfig {
        private String commandTopic = "orchestrator.commands";
        /** Reply topic for step results. Enables decoupled executor/orchestrator pattern.
         *  Defaults to commandTopic + ".replies". Set to empty string to disable (inline mode). */
        private String replyTopic;

        /** Resolve reply topic — defaults to commandTopic + ".replies" if not explicitly set. */
        public String getReplyTopic() {
            if (replyTopic == null) return commandTopic + ".replies";
            return replyTopic;
        }

        /** True if reply topic is enabled (default). Set reply-topic="" to disable. */
        public boolean isReplyEnabled() {
            return replyTopic == null || !replyTopic.isEmpty();
        }
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
        /** Include full flow state JSON (before/after) in step logs. Disable for performance at scale.
         *  With 5 steps per flow and ~2KB per snapshot:
         *    OFF: ~500 bytes/step = 2.5KB/flow = 2.5GB per million flows
         *    ON:  ~4KB/step = 20KB/flow = 20GB per million flows
         *  Use retention.step-log-days to auto-expire old logs. */
        private boolean includeFlowState = false;
    }

    /**
     * Per-flow configuration override. Key in the flows map matches @Flow(name="...").
     * Only non-null fields override the global config.
     *
     * Example:
     * <pre>
     * orchestrator:
     *   kafka:
     *     command-topic: orchestrator.commands  # shared default
     *   flows:
     *     payment:
     *       topic: payment.commands             # own topic
     *       dlt-topic: payment.critical-dlt     # custom DLT
     *     enigio:
     *       retry:
     *         max-attempts: 6                   # per-flow retry
     * </pre>
     */
    @Data
    public static class FlowConfig {
        /** Command topic override. Null = use global orchestrator.kafka.command-topic. */
        private String topic;
        /** DLT topic override. Null = use standard {topic}-dlt suffix. */
        private String dltTopic;
        /** Reply topic override. Null = use standard {topic}.replies suffix. */
        private String replyTopic;
        /** Per-flow retry config override. Null = use global orchestrator.retry. */
        private RetryConfig retry;
    }
}
