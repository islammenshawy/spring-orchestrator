package com.orchestrator.starter.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Auto-configures MongoDB transactions for replica set deployments.
 *
 * Enable with: orchestrator.mongodb.transactions-enabled=true
 *
 * When enabled:
 *   FlowOrchestrator wraps flow save + outbox event in a single
 *   MongoDB transaction — both commit or neither. Fully atomic.
 *
 * When disabled (default):
 *   Two sequential writes to the same MongoDB. The outbox publisher
 *   and recovery service handle the rare crash between them.
 *
 * The user never writes @Transactional — the library handles it internally.
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

    @Bean
    @ConditionalOnBean(MongoTransactionManager.class)
    @ConditionalOnMissingBean(TransactionTemplate.class)
    public TransactionTemplate mongoTransactionTemplate(MongoTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }
}
