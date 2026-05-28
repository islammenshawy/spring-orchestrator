package com.orchestrator.starter.failover;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Kafka listener containers for both DCs using warm standby.
 *
 * At startup: creates containers for ALL DCs with resolved topic names.
 * Only the active DC's containers are started. Standby containers exist
 * but don't poll — they hold no partitions and consume no resources.
 *
 * On failover: stop active containers, start standby containers.
 * No recreation, no factory rebuild. Swap time: ~100ms stop + ~5s join/seek.
 */
@Slf4j
public class DcAwareListenerManager {

    private final DcAwareKafkaManager kafkaManager;
    private final TopicResolver topicResolver;

    /** Per-DC containers: dcId → list of containers for that DC. */
    private final Map<String, List<ConcurrentMessageListenerContainer<String, String>>> dcContainers =
            new ConcurrentHashMap<>();

    private final List<ContainerBlueprint> blueprints = new ArrayList<>();
    private org.springframework.kafka.listener.ConsumerAwareRebalanceListener rebalanceListener;

    public DcAwareListenerManager(DcAwareKafkaManager kafkaManager, TopicResolver topicResolver) {
        this.kafkaManager = kafkaManager;
        this.topicResolver = topicResolver;
    }

    public void setRebalanceListener(org.springframework.kafka.listener.ConsumerAwareRebalanceListener listener) {
        this.rebalanceListener = listener;
    }

    /**
     * Register a container blueprint and create containers for ALL DCs.
     * Returns the active DC's container (already started).
     * Standby DC containers are created but not started.
     */
    public ConcurrentMessageListenerContainer<String, String> registerAndCreate(ContainerBlueprint blueprint) {
        blueprints.add(blueprint);
        String activeDc = kafkaManager.getActiveDc();
        ConcurrentMessageListenerContainer<String, String> activeContainer = null;

        for (var entry : kafkaManager.getAllDcIds()) {
            String dcId = entry;
            String resolvedTopic = resolveTopic(blueprint.getOriginalTopic(), dcId);

            @SuppressWarnings("unchecked")
            ConsumerFactory<String, String> factory = kafkaManager.getConsumerFactory(dcId);

            ContainerProperties props = new ContainerProperties(resolvedTopic);
            props.setGroupId(blueprint.getGroupId());
            props.setMessageListener(blueprint.getMessageListener());
            if (rebalanceListener != null) {
                props.setConsumerRebalanceListener(rebalanceListener);
            }

            var container = new ConcurrentMessageListenerContainer<>(factory, props);
            container.setBeanName(blueprint.getId() + "-" + dcId);
            container.setConcurrency(blueprint.getConcurrency() > 0 ? blueprint.getConcurrency() : 1);
            container.setAutoStartup(false); // we control lifecycle

            dcContainers.computeIfAbsent(dcId, k -> new ArrayList<>()).add(container);

            if (dcId.equals(activeDc)) {
                container.start();
                activeContainer = container;
                log.info("[DC-Listener] STARTED {}: {} → '{}'", dcId, blueprint.getId(), resolvedTopic);
            } else {
                log.info("[DC-Listener] STANDBY {}: {} → '{}' (created, not polling)", dcId, blueprint.getId(), resolvedTopic);
            }
        }

        return activeContainer;
    }

    /**
     * Switch all listener containers to a new DC.
     * Stops active DC's containers, starts standby DC's containers.
     * No recreation — containers were pre-created at startup.
     */
    public void switchDc(String fromDc, String toDc) {
        log.info("[DC-Listener] Switching containers: {} → {}", fromDc, toDc);

        // Stop active DC's containers
        List<ConcurrentMessageListenerContainer<String, String>> oldContainers =
                dcContainers.getOrDefault(fromDc, List.of());
        for (var container : oldContainers) {
            try {
                container.stop();
                log.info("[DC-Listener] Stopped: {}", container.getBeanName());
            } catch (Exception e) {
                log.warn("[DC-Listener] Failed to stop {}: {}", container.getBeanName(), e.getMessage());
            }
        }

        // Start standby DC's containers
        List<ConcurrentMessageListenerContainer<String, String>> newContainers =
                dcContainers.getOrDefault(toDc, List.of());
        for (var container : newContainers) {
            try {
                container.start();
                log.info("[DC-Listener] Started: {}", container.getBeanName());
            } catch (Exception e) {
                log.error("[DC-Listener] Failed to start {}: {}", container.getBeanName(), e.getMessage());
            }
        }

        log.info("[DC-Listener] Switch complete: {} containers stopped, {} started",
                oldContainers.size(), newContainers.size());
    }

    private String resolveTopic(String originalTopic, String targetDc) {
        String activeDc = kafkaManager.getActiveDc();
        // For standby DC: messages originate from the active DC
        return topicResolver.resolve(originalTopic, targetDc, activeDc);
    }

    public void stopAll() {
        dcContainers.values().forEach(containers ->
                containers.forEach(c -> { try { c.stop(); } catch (Exception ignored) {} }));
    }
}
