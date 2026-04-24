package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.outbox.OutboxPublisher;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import com.orchestrator.starter.retry.JitteredExponentialBackOffPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.List;

/**
 * Auto-configuration for the orchestrator starter.
 *
 * Automatically wires:
 * - StepRegistry (discovers all StepHandler beans, orders by getOrder())
 * - FlowOrchestrator (generic engine)
 * - IdempotencyService (two-layer dedup)
 * - Kafka retry topics with jitter (configurable via properties)
 * - Stale flow recovery service
 * - Kafka consumer + DLT handler
 *
 * Users provide:
 * - StepHandler implementations (@Component)
 * - OrchestratorFlowRepository implementation
 * - Flow entity implementing OrchestratorFlow
 * - application.yml config
 */
@Slf4j
@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorAutoConfiguration {

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

        log.info("Orchestrator configured: topic={}, steps={}, outbox=enabled",
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

    @Bean
    public RetryTopicConfiguration orchestratorRetryTopicConfig(
            KafkaTemplate<String, String> template,
            OrchestratorProperties props) {

        OrchestratorProperties.RetryConfig retry = props.getRetry();
        log.info("Orchestrator retry: attempts={}, delay={}ms, multiplier={}, maxDelay={}ms, jitter={}",
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

    /**
     * Kafka listener adapter — bridges the generic consumer to Spring Kafka.
     * Registers listeners for the command topic and its DLT.
     */
    @Bean
    public OrchestratorKafkaListenerAdapter orchestratorKafkaListenerAdapter(
            OrchestratorKafkaConsumer<?> consumer,
            OrchestratorProperties props) {
        return new OrchestratorKafkaListenerAdapter(consumer, props.getKafka().getCommandTopic());
    }

    /**
     * Inner class that holds the @KafkaListener annotations.
     * SpEL resolves the topic names from properties.
     */
    public static class OrchestratorKafkaListenerAdapter {

        private final OrchestratorKafkaConsumer<?> consumer;
        private final String commandTopic;

        public OrchestratorKafkaListenerAdapter(OrchestratorKafkaConsumer<?> consumer, String commandTopic) {
            this.consumer = consumer;
            this.commandTopic = commandTopic;
        }

        @KafkaListener(topics = "${orchestrator.kafka.command-topic}", groupId = "${spring.application.name:orchestrator}-processor")
        public void onCommand(String payload,
                              @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onStepCommand(payload, topic, offset);
        }

        @KafkaListener(topics = "${orchestrator.kafka.command-topic}-dlt", groupId = "${spring.application.name:orchestrator}-dlt")
        public void onDlt(String payload,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onDlt(payload, topic, offset);
        }
    }
}
