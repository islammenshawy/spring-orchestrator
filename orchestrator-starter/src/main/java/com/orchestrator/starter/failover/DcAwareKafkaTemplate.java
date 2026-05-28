package com.orchestrator.starter.failover;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * KafkaTemplate wrapper that routes sends to the active DC's template.
 * Registered as @Primary bean when failover is enabled — all existing code
 * uses this transparently via Spring injection.
 *
 * On DC swap, all subsequent sends go to the new DC's Kafka cluster.
 */
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked"})
public class DcAwareKafkaTemplate extends KafkaTemplate {

    private final DcAwareKafkaManager manager;

    public DcAwareKafkaTemplate(DcAwareKafkaManager manager) {
        super(new DefaultKafkaProducerFactory(Map.of()));
        this.manager = manager;
    }

    @Override
    public CompletableFuture send(String topic, Object key, Object data) {
        String resolvedTopic = manager.resolveActiveTopic(topic);
        KafkaTemplate active = manager.getActiveTemplate();
        return active.send(resolvedTopic, key, data);
    }

    @Override
    public CompletableFuture send(String topic, Integer partition, Object key, Object data) {
        String resolvedTopic = manager.resolveActiveTopic(topic);
        KafkaTemplate active = manager.getActiveTemplate();
        return active.send(resolvedTopic, partition, key, data);
    }

    @Override
    public CompletableFuture send(String topic, Object data) {
        String resolvedTopic = manager.resolveActiveTopic(topic);
        KafkaTemplate active = manager.getActiveTemplate();
        return active.send(resolvedTopic, data);
    }
}
