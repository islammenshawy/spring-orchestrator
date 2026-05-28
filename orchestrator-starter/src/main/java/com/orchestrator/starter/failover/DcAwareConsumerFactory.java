package com.orchestrator.starter.failover;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * ConsumerFactory wrapper that delegates to the active DC's factory.
 * On failover, new consumers are created from the new DC's factory.
 */
@Slf4j
public class DcAwareConsumerFactory<K, V> implements ConsumerFactory<K, V> {

    private final DcAwareKafkaManager manager;

    public DcAwareConsumerFactory(DcAwareKafkaManager manager) {
        this.manager = manager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Consumer<K, V> createConsumer(String groupId, String clientIdPrefix,
                                          String clientIdSuffix, Properties properties) {
        ConsumerFactory<K, V> activeFactory = (ConsumerFactory<K, V>) manager.getActiveConsumerFactory();
        log.debug("[DC-Consumer] Creating consumer on DC '{}' group={}", manager.getActiveDc(), groupId);
        return activeFactory.createConsumer(groupId, clientIdPrefix, clientIdSuffix, properties);
    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    @Override
    public Map<String, Object> getConfigurationProperties() {
        return manager.getActiveConsumerFactory().getConfigurationProperties();
    }
}
