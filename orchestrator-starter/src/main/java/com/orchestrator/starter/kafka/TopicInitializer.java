package com.orchestrator.starter.kafka;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.SmartLifecycle;

import java.util.*;

/**
 * Creates Kafka topics with correct partition counts at startup.
 * Runs at SmartLifecycle phase 0, before Kafka listener containers (phase MAX_VALUE).
 * Idempotent: ignores existing topics.
 *
 * Topics created:
 * - command topic: partitionCount partitions (e.g., 6)
 * - retry topics (-retry-0, -retry-1, ...): retryPartitionCount (e.g., 3)
 * - DLT topic (-dlt): retryPartitionCount
 */
@Slf4j
@RequiredArgsConstructor
public class TopicInitializer implements SmartLifecycle {

    private final OrchestratorProperties props;
    private final String bootstrapServers;
    private volatile boolean running = false;

    @Override
    public void start() {
        if (!props.getKafka().isCreateTopics()) {
            log.info("[TopicInit] Topic creation disabled");
            running = true;
            return;
        }

        OrchestratorProperties.KafkaConfig kafka = props.getKafka();
        OrchestratorProperties.RetryConfig retry = props.getRetry();
        String baseTopic = kafka.getCommandTopic();

        List<NewTopic> topics = new ArrayList<>();

        // Command topic — full partition count
        topics.add(buildTopic(baseTopic, kafka.getPartitionCount(),
                kafka.getReplicationFactor(), kafka.getMinInsyncReplicas()));

        // Retry topics — fewer partitions
        int retryCount = retry.getMaxAttempts() - 1;
        for (int i = 0; i < retryCount; i++) {
            topics.add(buildTopic(baseTopic + "-retry-" + i, kafka.getRetryPartitionCount(),
                    kafka.getReplicationFactor(), kafka.getMinInsyncReplicas()));
        }

        // DLT topic
        topics.add(buildTopic(baseTopic + "-dlt", kafka.getRetryPartitionCount(),
                kafka.getReplicationFactor(), kafka.getMinInsyncReplicas()));

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> toCreate = topics.stream()
                    .filter(t -> !existing.contains(t.name()))
                    .toList();

            if (toCreate.isEmpty()) {
                log.info("[TopicInit] All topics already exist");
            } else {
                admin.createTopics(toCreate).all().get();
                toCreate.forEach(t -> log.info("[TopicInit] Created topic {} (partitions={}, replicas={})",
                        t.name(), t.numPartitions(), t.replicationFactor()));
            }
        } catch (Exception e) {
            log.error("[TopicInit] Failed to create topics: {}", e.getMessage());
        }

        running = true;
    }

    private NewTopic buildTopic(String name, int partitions, short replicas, int minIsr) {
        NewTopic topic = new NewTopic(name, partitions, replicas);
        if (minIsr > 1) {
            topic.configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minIsr)));
        }
        return topic;
    }

    @Override public void stop() { running = false; }
    @Override public boolean isRunning() { return running; }
    @Override public int getPhase() { return 0; } // Run before Kafka listeners
}
