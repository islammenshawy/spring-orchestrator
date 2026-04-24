package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import com.orchestrator.starter.kafka.KafkaMetricsService;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.orchestrator.starter.kafka.OrchestratorRebalanceListener;
import com.orchestrator.starter.kafka.PartitionAssignmentTracker;
import com.orchestrator.starter.kafka.TopicInitializer;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.outbox.OutboxPublisher;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import com.orchestrator.starter.retry.JitteredExponentialBackOffPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorAutoConfiguration {

    // ========== Core Beans ==========

    @Bean
    @ConditionalOnMissingBean
    public IdempotencyService orchestratorIdempotencyService(ProcessedEventRepository repository) {
        return new IdempotencyService(repository);
    }

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("rawtypes")
    public StepRegistry<?> orchestratorStepRegistry(List<StepHandler> handlers) {
        return new StepRegistry<>(handlers);
    }

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    public FlowOrchestrator<?> orchestratorFlowOrchestrator(
            OrchestratorFlowRepository<?> flowRepository,
            StepRegistry<?> stepRegistry,
            OutboxEventRepository outboxRepository,
            ObjectMapper objectMapper,
            OrchestratorProperties props) {
        log.info("Orchestrator: topic={}, steps={}, outbox=enabled",
                props.getKafka().getCommandTopic(), stepRegistry.getStepNames());
        return new FlowOrchestrator(
                flowRepository, stepRegistry, outboxRepository, objectMapper,
                props.getKafka().getCommandTopic());
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisher orchestratorOutboxPublisher(
            OutboxEventRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate) {
        return new OutboxPublisher(outboxRepository, kafkaTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    public OrchestratorKafkaConsumer<?> orchestratorKafkaConsumer(
            FlowOrchestrator<?> orchestrator,
            IdempotencyService idempotencyService,
            ObjectMapper objectMapper) {
        return new OrchestratorKafkaConsumer(orchestrator, idempotencyService, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    public StaleFlowRecoveryService<?> orchestratorRecoveryService(
            OrchestratorFlowRepository<?> flowRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            OrchestratorProperties props) {
        return new StaleFlowRecoveryService(
                flowRepository, kafkaTemplate, objectMapper,
                props.getKafka().getCommandTopic(),
                props.getRecovery().getStaleThresholdMinutes());
    }

    // ========== Topic Initialization ==========

    @Bean
    @ConditionalOnMissingBean
    public TopicInitializer orchestratorTopicInitializer(
            OrchestratorProperties props,
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        return new TopicInitializer(props, bootstrapServers);
    }

    // ========== Rebalance Handling ==========

    @Bean
    @ConditionalOnMissingBean
    public PartitionAssignmentTracker orchestratorPartitionTracker() {
        return new PartitionAssignmentTracker();
    }

    @Bean
    @ConditionalOnMissingBean
    public OrchestratorRebalanceListener orchestratorRebalanceListener(
            PartitionAssignmentTracker tracker) {
        return new OrchestratorRebalanceListener(tracker);
    }

    @Bean(name = "orchestratorKafkaListenerContainerFactory")
    @ConditionalOnMissingBean(name = "orchestratorKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> orchestratorKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            OrchestratorRebalanceListener rebalanceListener,
            OrchestratorProperties props) {

        OrchestratorProperties.KafkaConfig kafka = props.getKafka();

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(kafka.getConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);

        // Static group membership for Kubernetes
        if (kafka.isStaticMembership()) {
            String hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");
            String instanceId = kafka.getInstanceIdPrefix() + hostname;

            Map<String, Object> consumerProps = new HashMap<>();
            consumerProps.put("group.instance.id", instanceId);
            consumerProps.put("session.timeout.ms", kafka.getSessionTimeoutMs());
            factory.getContainerProperties().setKafkaConsumerProperties(
                    propsFromMap(consumerProps));

            log.info("Static membership: group.instance.id={}, sessionTimeout={}ms",
                    instanceId, kafka.getSessionTimeoutMs());
        }

        // Client rack for reading from closest replica
        if (kafka.getClientRack() != null && !kafka.getClientRack().isBlank()) {
            Map<String, Object> rackProps = new HashMap<>();
            rackProps.put("client.rack", kafka.getClientRack());
            factory.getContainerProperties().setKafkaConsumerProperties(
                    propsFromMap(rackProps));
            log.info("Client rack: {}", kafka.getClientRack());
        }

        log.info("Consumer factory: concurrency={}, ack=RECORD, rebalanceListener=enabled",
                kafka.getConcurrency());

        return factory;
    }

    // ========== Metrics (optional, conditional on Micrometer) ==========

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public KafkaMetricsService orchestratorKafkaMetrics(
            MeterRegistry registry,
            PartitionAssignmentTracker tracker) {
        log.info("Kafka metrics enabled");
        return new KafkaMetricsService(registry, tracker);
    }

    // ========== Retry Topics ==========

    @Bean
    public RetryTopicConfiguration orchestratorRetryTopicConfig(
            KafkaTemplate<String, String> template,
            OrchestratorProperties props) {

        OrchestratorProperties.RetryConfig retry = props.getRetry();
        log.info("Retry: attempts={}, delay={}ms, multiplier={}, maxDelay={}ms, jitter={}",
                retry.getMaxAttempts(), retry.getInitialIntervalMs(),
                retry.getMultiplier(), retry.getMaxIntervalMs(), retry.getJitterFactor());

        JitteredExponentialBackOffPolicy backoff = new JitteredExponentialBackOffPolicy();
        backoff.setInitialInterval(retry.getInitialIntervalMs());
        backoff.setMultiplier(retry.getMultiplier());
        backoff.setMaxInterval(retry.getMaxIntervalMs());
        backoff.setJitterFactor(retry.getJitterFactor());

        return RetryTopicConfigurationBuilder
                .newInstance()
                .customBackoff(backoff)
                .maxAttempts(retry.getMaxAttempts())
                .includeTopic(props.getKafka().getCommandTopic())
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .dltProcessingFailureStrategy(DltStrategy.ALWAYS_RETRY_ON_ERROR)
                .retryOn(RetryableStepException.class)
                .create(template);
    }

    // ========== Kafka Listeners ==========

    @Bean
    public OrchestratorKafkaListenerAdapter orchestratorKafkaListenerAdapter(
            OrchestratorKafkaConsumer<?> consumer) {
        return new OrchestratorKafkaListenerAdapter(consumer);
    }

    public static class OrchestratorKafkaListenerAdapter {

        private final OrchestratorKafkaConsumer<?> consumer;

        public OrchestratorKafkaListenerAdapter(OrchestratorKafkaConsumer<?> consumer) {
            this.consumer = consumer;
        }

        @KafkaListener(
                topics = "${orchestrator.kafka.command-topic}",
                groupId = "${spring.application.name:orchestrator}-processor",
                containerFactory = "orchestratorKafkaListenerContainerFactory")
        public void onCommand(String payload,
                              @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onStepCommand(payload, topic, offset);
        }

        @KafkaListener(
                topics = "${orchestrator.kafka.command-topic}-dlt",
                groupId = "${spring.application.name:orchestrator}-dlt",
                containerFactory = "orchestratorKafkaListenerContainerFactory")
        public void onDlt(String payload,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onDlt(payload, topic, offset);
        }
    }

    private java.util.Properties propsFromMap(Map<String, Object> map) {
        java.util.Properties p = new java.util.Properties();
        p.putAll(map);
        return p;
    }
}
