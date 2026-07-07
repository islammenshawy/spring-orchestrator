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

    /**
     * Used by Spring Kafka's DeadLetterPublishingRecoverer for retry-topic and DLT routing.
     * The record's topic is already the exact destination (derived from the CONSUMED topic,
     * including any dc-prefix), so it must NOT be re-resolved — only transported on the active DC.
     *
     * Without this override the call fell through to the dummy super() producer (empty config →
     * localhost:9092) and EVERY retry/DLT publication failed in failover mode: the error handler
     * then fell back to in-place seeks, the redelivery hit the WAITING_RETRY skip-guard, the offset
     * committed, and the scanner re-drove the step forever — unbounded retries, nothing ever
     * reaching the retry topics or DLT.
     */
    @Override
    public CompletableFuture send(org.apache.kafka.clients.producer.ProducerRecord record) {
        return manager.getActiveTemplate().send(record);
    }

    /** Same transport routing for Message-based sends (topic resolved from headers by the target). */
    @Override
    public CompletableFuture send(org.springframework.messaging.Message message) {
        return manager.getActiveTemplate().send(message);
    }
}
