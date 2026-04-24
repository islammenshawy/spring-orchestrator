package com.enigio.orchestrator.common.config;

import com.mongodb.client.MongoClient;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

/**
 * Enables MongoDB multi-document transactions when running against a replica set.
 *
 * Standalone MongoDB does not support transactions. In that case, atomicity is
 * achieved through:
 * - Outbox pattern: flow state and outbox event are separate writes. If the
 *   container crashes between them, recovery service detects stale flows.
 * - Consumer idempotency: duplicate messages are caught by processed_events collection.
 *
 * For production (replica set), enable with: mongo.transactions.enabled=true
 * This gives full atomicity between flow state + outbox event writes.
 */
@Slf4j
@Configuration
public class MongoTransactionConfig {

    @Bean
    @ConditionalOnProperty(name = "mongo.transactions.enabled", havingValue = "true", matchIfMissing = false)
    MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        log.info("MongoDB transactions ENABLED (replica set mode)");
        return new MongoTransactionManager(dbFactory);
    }
}
