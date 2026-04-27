package com.orchestrator.starter.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Validates at startup that Kafka is reachable and required topics can be resolved.
 * Fails application startup if Kafka is unreachable — the library cannot function
 * without Kafka, so failing fast is safer than starting in a broken state.
 *
 * Topics themselves are auto-created by Spring Kafka on first use, so we don't
 * fail on missing topics — but we DO fail if Kafka itself is down.
 */
@Slf4j
public class TopicValidator {

    private final KafkaAdmin kafkaAdmin;
    private final OrchestratorProperties props;

    public TopicValidator(KafkaAdmin kafkaAdmin, OrchestratorProperties props) {
        this.kafkaAdmin = kafkaAdmin;
        this.props = props;
    }

    @EventListener(ApplicationStartedEvent.class)
    public void validateTopics() {
        List<String> requiredTopics = new ArrayList<>();
        requiredTopics.add(props.getKafka().getCommandTopic());
        if (props.getKafka().isReplyEnabled()) {
            requiredTopics.add(props.getKafka().getReplyTopic());
        }

        Set<String> existingTopics;
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            existingTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Orchestrator startup failed: cannot connect to Kafka. " +
                    "Ensure Kafka is running and spring.kafka.bootstrap-servers is correct. " +
                    "Error: " + e.getMessage(), e);
        }

        List<String> missing = requiredTopics.stream()
                .filter(t -> !existingTopics.contains(t))
                .toList();

        if (missing.isEmpty()) {
            log.info("Kafka topic validation passed: {} topics verified", requiredTopics);
        } else {
            // Topics will be auto-created on first use by Spring Kafka.
            // Log as info, not error — this is normal on first deployment.
            log.info("Kafka topics {} not yet created — will be auto-created on first message", missing);
        }

        // Log all orchestrator-related topics for visibility
        List<String> orchTopics = existingTopics.stream()
                .filter(t -> t.startsWith(props.getKafka().getCommandTopic()))
                .sorted()
                .toList();
        if (!orchTopics.isEmpty()) {
            log.info("Orchestrator Kafka topics: {}", orchTopics);
        }
    }
}
