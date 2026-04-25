package com.orchestrator.starter.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

/**
 * Auto-configures MongoDB transaction manager for replica set deployments.
 *
 * Enabled with: orchestrator.mongodb.transactions-enabled=true
 * Requires MongoDB replica set (standalone doesn't support transactions).
 *
 * When enabled, the outbox pattern becomes fully atomic:
 * flow save + outbox event = single transaction, both or neither.
 *
 * When disabled (default): two sequential writes to the same MongoDB,
 * microseconds apart. The outbox publisher and recovery service handle
 * the rare case where the second write is lost.
 */
@Slf4j
@AutoConfiguration
public class MongoTransactionAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(MongoTransactionManager.class)
    @ConditionalOnProperty(name = "orchestrator.mongodb.transactions-enabled", havingValue = "true")
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory dbFactory) {
        log.info("MongoDB transactions enabled (replica set mode)");
        return new MongoTransactionManager(dbFactory);
    }
}
