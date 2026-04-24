package com.orchestrator.starter.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

    private KafkaConfig kafka = new KafkaConfig();
    private RetryConfig retry = new RetryConfig();
    private RecoveryConfig recovery = new RecoveryConfig();

    @Data
    public static class KafkaConfig {
        /** Kafka topic for step commands */
        private String commandTopic = "orchestrator.commands";

        /** Number of partitions for the command topic (match container count) */
        private int partitionCount = 6;

        /** Number of partitions for retry/DLT topics (lower throughput) */
        private int retryPartitionCount = 3;

        /** Replication factor: 1 for dev, 3 for prod */
        private short replicationFactor = 1;

        /** Minimum in-sync replicas: 1 for dev, 2 for prod */
        private int minInsyncReplicas = 1;

        /** Auto-create topics at startup. Set false if infra team manages topics */
        private boolean createTopics = true;

        /** Concurrent consumer threads per instance (should be <= partitionCount) */
        private int concurrency = 3;

        /** Enable static group membership for Kubernetes StatefulSet pods */
        private boolean staticMembership = false;

        /** Prefix for group.instance.id (appended with HOSTNAME env var) */
        private String instanceIdPrefix = "orch-";

        /** Session timeout for static membership (must exceed pod restart time) */
        private int sessionTimeoutMs = 45000;

        /** Client rack for reading from closest replica (set to AZ name in prod) */
        private String clientRack = "";
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
