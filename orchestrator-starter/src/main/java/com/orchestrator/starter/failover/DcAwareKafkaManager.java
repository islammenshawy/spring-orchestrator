package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Kafka consumer/producer pairs for multiple DCs.
 * Both are created at startup (warm standby). Only one is active at a time.
 *
 * On failover: stop active consumer, start standby consumer. ~100ms swap.
 * No factory rebuild, no cold connect, no rebalance delay.
 */
@Slf4j
public class DcAwareKafkaManager {

    private final Map<String, ConsumerFactory<String, String>> consumerFactories = new ConcurrentHashMap<>();
    private final Map<String, ProducerFactory<String, String>> producerFactories = new ConcurrentHashMap<>();
    private final Map<String, KafkaTemplate<String, String>> kafkaTemplates = new ConcurrentHashMap<>();
    private final TopicResolver topicResolver;
    private final OrchestratorProperties.FailoverConfig config;

    @Getter private volatile String activeDc;
    private volatile String originatingDc; // DC where messages were originally produced

    /** Set by FailoverAutoConfiguration after context is ready. */
    @Setter private DcAwareListenerManager listenerManager;
    /** Registry for @KafkaListener containers (command topic + retry topics). */
    @Setter private org.springframework.kafka.config.KafkaListenerEndpointRegistry kafkaListenerRegistry;

    public DcAwareKafkaManager(OrchestratorProperties.FailoverConfig config, TopicResolver topicResolver) {
        this.config = config;
        this.topicResolver = topicResolver;
        this.activeDc = config.getActiveDc();
        this.originatingDc = config.getActiveDc();

        // Create factories for all configured DCs (warm standby)
        config.getDcs().forEach((dcId, dcConfig) -> {
            consumerFactories.put(dcId, createConsumerFactory(dcId, dcConfig));
            ProducerFactory<String, String> pf = createProducerFactory(dcId, dcConfig);
            producerFactories.put(dcId, pf);
            kafkaTemplates.put(dcId, new KafkaTemplate<>(pf));
            log.info("[DC-Kafka] Created factories for DC '{}' → {}", dcId, dcConfig.getBootstrap());
        });
    }

    /**
     * Switch the active DC. Stops all Kafka listener containers (they'll reconnect
     * on next start using the offset recovery listener which reads from MongoDB).
     * The DcAwareKafkaTemplate automatically routes to the new active template.
     */
    public void switchActiveDc(String newActiveDc) {
        if (!consumerFactories.containsKey(newActiveDc)) {
            throw new IllegalArgumentException("Unknown DC: " + newActiveDc + ". Known: " + consumerFactories.keySet());
        }
        String previousDc = this.activeDc;
        this.originatingDc = previousDc;
        this.activeDc = newActiveDc;

        // Switch programmatic listener containers (reply + DLT): stop old DC's, start new DC's
        if (listenerManager != null) {
            listenerManager.switchDc(previousDc, newActiveDc);
        }

        // Restart @KafkaListener containers (command topic + retry topics).
        // DcAwareConsumerFactory is @Primary — restarted containers create consumers from new DC.
        if (kafkaListenerRegistry != null) {
            log.info("[DC-Kafka] Restarting @KafkaListener containers for DC switch...");
            kafkaListenerRegistry.getListenerContainers().forEach(container -> {
                try {
                    String id = container.getListenerId();
                    container.stop();
                    container.start();
                    log.info("[DC-Kafka] Restarted @KafkaListener: {}", id);
                } catch (Exception e) {
                    log.warn("[DC-Kafka] Failed to restart container: {}", e.getMessage());
                }
            });
        }

        // Producer swap is immediate — DcAwareKafkaTemplate reads activeDc on each send()
        log.info("[DC-Kafka] Switched active DC: {} → {} (producer + all consumers)", previousDc, newActiveDc);
    }

    /** Get the active KafkaTemplate for producing messages. */
    public KafkaTemplate<String, String> getActiveTemplate() {
        return kafkaTemplates.get(activeDc);
    }

    /** Get the KafkaTemplate for a specific DC. */
    public KafkaTemplate<String, String> getTemplate(String dcId) {
        return kafkaTemplates.get(dcId);
    }

    /** Get the active ConsumerFactory. */
    public ConsumerFactory<String, String> getActiveConsumerFactory() {
        return consumerFactories.get(activeDc);
    }

    /** Get the ConsumerFactory for a specific DC. */
    public ConsumerFactory<String, String> getConsumerFactory(String dcId) {
        return consumerFactories.get(dcId);
    }

    /** Get all configured DC identifiers. */
    public java.util.Set<String> getAllDcIds() {
        return consumerFactories.keySet();
    }

    /**
     * Resolve a topic name for the currently active DC.
     * Handles MM2 prefix mapping when reading another DC's replicated data.
     */
    public String resolveActiveTopic(String originalTopic) {
        return topicResolver.resolve(originalTopic, activeDc, originatingDc);
    }

    /** Get the originating DC (where messages were originally produced). */
    public String getOriginatingDc() {
        return originatingDc;
    }

    private ConsumerFactory<String, String> createConsumerFactory(String dcId,
                                                                    OrchestratorProperties.DcConfig dcConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dcConfig.getBootstrap());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "orchestrator-" + dcId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private ProducerFactory<String, String> createProducerFactory(String dcId,
                                                                    OrchestratorProperties.DcConfig dcConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dcConfig.getBootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "orchestrator-producer-" + dcId);
        return new DefaultKafkaProducerFactory<>(props);
    }

    public void close() {
        producerFactories.values().forEach(pf -> {
            try { ((DefaultKafkaProducerFactory<?, ?>) pf).destroy(); } catch (Exception ignored) {}
        });
    }
}
