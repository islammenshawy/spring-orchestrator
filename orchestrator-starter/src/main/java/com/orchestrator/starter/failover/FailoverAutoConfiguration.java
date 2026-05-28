package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
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
    public DcFailoverSupervisor dcFailoverSupervisor(DcHealthProbe probe,
                                                      DcAwareKafkaManager kafkaManager,
                                                      MongoTemplate mongoTemplate,
                                                      OrchestratorProperties props) {
        return new DcFailoverSupervisor(probe, kafkaManager, mongoTemplate, props.getFailover());
    }
}
