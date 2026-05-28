package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for multi-DC failover.
 * Only active when orchestrator.failover.enabled=true.
 *
 * Creates:
 * - TopicResolver (IDENTITY or PREFIXED)
 * - DcHealthProbe (AdminClient per DC)
 * - DcAwareKafkaManager (warm standby consumer/producer pairs)
 * - DcFailoverSupervisor (health probe + state machine + swap)
 */
@Slf4j
@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "orchestrator.failover", name = "enabled", havingValue = "true")
public class FailoverAutoConfiguration {

    @Bean
    public TopicResolver topicResolver(OrchestratorProperties props) {
        return new TopicResolver(props.getFailover());
    }

    @Bean
    public DcHealthProbe dcHealthProbe(OrchestratorProperties props) {
        var failoverConfig = props.getFailover();
        Map<String, String> dcBootstraps = new HashMap<>();
        failoverConfig.getDcs().forEach((dcId, dcConfig) ->
                dcBootstraps.put(dcId, dcConfig.getBootstrap()));

        log.info("[Failover] Health probe configured for DCs: {}", dcBootstraps.keySet());
        return new DcHealthProbe(dcBootstraps, failoverConfig.getProbeTimeoutMs());
    }

    @Bean
    public DcAwareKafkaManager dcAwareKafkaManager(OrchestratorProperties props, TopicResolver topicResolver) {
        return new DcAwareKafkaManager(props.getFailover(), topicResolver);
    }

    @Bean
    public DcAwareListenerManager dcAwareListenerManager(DcAwareKafkaManager kafkaManager,
                                                          TopicResolver topicResolver,
                                                          org.springframework.kafka.config.KafkaListenerEndpointRegistry registry) {
        var manager = new DcAwareListenerManager(kafkaManager, topicResolver);
        kafkaManager.setListenerManager(manager);
        kafkaManager.setKafkaListenerRegistry(registry);
        return manager;
    }

    @Bean
    public DcFailoverSupervisor dcFailoverSupervisor(DcHealthProbe probe,
                                                      DcAwareKafkaManager kafkaManager,
                                                      MongoTemplate mongoTemplate,
                                                      OrchestratorProperties props) {
        return new DcFailoverSupervisor(probe, kafkaManager, mongoTemplate, props.getFailover());
    }

    /**
     * DC-aware KafkaTemplate — replaces the default KafkaTemplate bean.
     * All producers use this, which delegates to the active DC's real template.
     */
    @Bean
    @Primary
    @SuppressWarnings("rawtypes")
    public KafkaTemplate dcAwareKafkaTemplate(DcAwareKafkaManager kafkaManager) {
        log.info("[Failover] DcAwareKafkaTemplate registered as primary KafkaTemplate");
        return new DcAwareKafkaTemplate(kafkaManager);
    }

    /**
     * DC-aware ConsumerFactory — replaces the default ConsumerFactory bean.
     * When containers restart after failover, new consumers connect to the active DC.
     */
    @Bean
    @Primary
    @SuppressWarnings("rawtypes")
    public org.springframework.kafka.core.ConsumerFactory dcAwareConsumerFactory(DcAwareKafkaManager kafkaManager) {
        log.info("[Failover] DcAwareConsumerFactory registered as primary ConsumerFactory");
        return new DcAwareConsumerFactory(kafkaManager);
    }

    @Bean
    public DcHealthEndpoint dcHealthEndpoint(DcFailoverSupervisor supervisor,
                                              DcHealthProbe probe,
                                              DcAwareKafkaManager kafkaManager,
                                              TopicResolver topicResolver,
                                              OrchestratorProperties props) {
        return new DcHealthEndpoint(supervisor, probe, kafkaManager, topicResolver, props.getFailover());
    }
}
