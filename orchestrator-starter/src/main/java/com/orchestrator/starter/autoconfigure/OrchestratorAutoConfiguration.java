package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowDefinitionScanner;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
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
 * Only provides beans that don't exist in Spring Boot / Spring Kafka:
 * - StepRegistry, FlowOrchestrator, IdempotencyService (our domain logic)
 * - Outbox publisher (transactional outbox pattern)
 * - Stale flow recovery (container crash safety net)
 * - Retry topic config with jittered backoff (Spring Kafka provides the infra,
 *   we configure it with our custom backoff policy)
 * - Kafka listener adapter (bridges our consumer to @KafkaListener)
 *
 * Everything else — concurrency, ack mode, rebalancing strategy, session timeout,
 * static membership, rack awareness, topic creation — is configured via standard
 * Spring Boot properties in application.yml. No custom code needed.
 */
@Slf4j
@AutoConfiguration
@EnableScheduling
@org.springframework.boot.autoconfigure.AutoConfigurationPackage
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public IdempotencyService orchestratorIdempotencyService(ProcessedEventRepository repository) {
        return new IdempotencyService(repository);
    }

    @Bean
    @ConditionalOnMissingBean
    public IndexInitializer orchestratorIndexInitializer(
            org.springframework.data.mongodb.core.MongoTemplate mongoTemplate,
            OrchestratorProperties props) {
        return new IndexInitializer(mongoTemplate, props);
    }

    /**
     * Auto-generates a repository if the user hasn't defined one.
     * Discovers the entity type from FlowDefinition<F> via reflection.
     */
    @Bean
    @ConditionalOnMissingBean(OrchestratorFlowRepository.class)
    @SuppressWarnings({"unchecked", "rawtypes"})
    public OrchestratorFlowRepository<?> orchestratorGenericFlowRepository(
            org.springframework.data.mongodb.core.MongoTemplate mongoTemplate,
            ApplicationContext context) {

        // Discover entity type from @Flow class's FlowDefinition<F> generic parameter
        Class<?> entityClass = discoverEntityType(context);
        log.info("Auto-generated repository for entity: {}", entityClass.getSimpleName());
        return new com.orchestrator.starter.domain.GenericFlowRepository(mongoTemplate, entityClass);
    }

    private Class<?> discoverEntityType(ApplicationContext context) {
        var flowBeans = context.getBeansWithAnnotation(
                com.orchestrator.starter.annotation.Flow.class);
        for (Object flowDef : flowBeans.values()) {
            Class<?> clazz = flowDef.getClass();
            java.lang.reflect.Type superclass = clazz.getGenericSuperclass();
            if (superclass instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type[] args = pt.getActualTypeArguments();
                if (args.length > 0 && args[0] instanceof Class<?> entityClass) {
                    return entityClass;
                }
            }
        }
        // Fallback
        return com.orchestrator.starter.domain.AbstractFlow.class;
    }

    /**
     * Step registry — discovers steps from two sources:
     * 1. @Flow classes with @Step methods (single-class approach)
     * 2. Individual StepHandler @Component beans (multi-class approach)
     * Both can coexist. @Flow steps are discovered first.
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("rawtypes")
    public StepRegistry<?> orchestratorStepRegistry(
            ApplicationContext context,
            List<StepHandler> handlers) {
        // Scan for @Flow classes with @Step methods
        List<StepHandler> flowSteps = FlowDefinitionScanner.scan(context);
        if (!flowSteps.isEmpty()) {
            return new StepRegistry<>(flowSteps);
        }
        // Fallback: individual StepHandler beans
        return new StepRegistry<>(handlers);
    }

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    public FlowOrchestrator<?> orchestratorFlowOrchestrator(
            OrchestratorFlowRepository<?> flowRepository,
            StepRegistry<?> stepRegistry,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper,
            OrchestratorProperties props,
            @Autowired(required = false) TransactionTemplate transactionTemplate) {

        boolean txEnabled = transactionTemplate != null;
        log.info("Saga orchestrator: topic={}, steps={}, outbox=enabled, transactions={}",
                props.getKafka().getCommandTopic(), stepRegistry.getStepNames(),
                txEnabled ? "ATOMIC" : "best-effort");

        return new FlowOrchestrator(
                flowRepository, stepRegistry, outboxRepository, stepLogRepository,
                objectMapper, props.getKafka().getCommandTopic(), transactionTemplate,
                props.getAudit().isIncludeFlowState());
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
                groupId = "${spring.application.name:orchestrator}-processor")
        public void onCommand(String payload,
                              @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onStepCommand(payload, topic, offset);
        }

        @KafkaListener(
                topics = "${orchestrator.kafka.command-topic}-dlt",
                groupId = "${spring.application.name:orchestrator}-dlt")
        public void onDlt(String payload,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
            consumer.onDlt(payload, topic, offset);
        }
    }
}
