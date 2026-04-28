package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.GenericFlowRepository;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.*;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.outbox.OutboxPublisher;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import com.orchestrator.starter.retry.JitteredExponentialBackOffPolicy;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Auto-configuration for the orchestrator starter.
 *
 * Supports multi-flow: multiple @Flow classes in one app, each with optional
 * per-flow topic/DLT/retry overrides. Default: all flows share one topic pair.
 *
 * Beans created:
 * - FlowTypeRegistry (holds per-flow StepRegistry, FlowOrchestrator, Repository)
 * - OrchestratorKafkaConsumer (routes by flowType from message)
 * - Programmatic Kafka listener containers (one per unique topic)
 * - RetryTopicConfiguration per unique topic
 * - Shared services: IdempotencyService, OutboxPublisher, IndexInitializer, TopicValidator
 */
@Slf4j
@AutoConfiguration
@EnableScheduling
@org.springframework.kafka.annotation.EnableKafkaRetryTopic
@org.springframework.boot.autoconfigure.AutoConfigurationPackage
@org.springframework.data.mongodb.repository.config.EnableMongoRepositories(basePackages = "com.orchestrator.starter")
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorAutoConfiguration {

    // ========== Shared services (not per-flow) ==========

    @Bean
    @ConditionalOnMissingBean
    public IdempotencyService orchestratorIdempotencyService(ProcessedEventRepository repository) {
        return new IdempotencyService(repository);
    }

    @Bean
    @ConditionalOnMissingBean
    public TopicValidator orchestratorTopicValidator(KafkaAdmin kafkaAdmin, OrchestratorProperties props) {
        return new TopicValidator(kafkaAdmin, props);
    }

    @Bean
    @ConditionalOnMissingBean
    public IndexInitializer orchestratorIndexInitializer(MongoTemplate mongoTemplate, OrchestratorProperties props) {
        return new IndexInitializer(mongoTemplate, props);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisher orchestratorOutboxPublisher(
            OutboxEventRepository outboxRepository,
            KafkaTemplate kafkaTemplate) {
        return new OutboxPublisher(outboxRepository, kafkaTemplate);
    }

    // ========== FlowTypeRegistry (the core multi-flow bean) ==========

    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    public FlowTypeRegistry orchestratorFlowTypeRegistry(
            ApplicationContext context,
            MongoTemplate mongoTemplate,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper,
            OrchestratorProperties props,
            KafkaTemplate kafkaTemplate,
            @Autowired(required = false) TransactionTemplate transactionTemplate) {

        // Scan all @Flow classes
        Map<String, FlowDefinitionScanner.FlowTypeInfo> flowTypes =
                FlowDefinitionScanner.scanByFlowType(context);

        if (flowTypes.isEmpty()) {
            // Fallback: check for individual StepHandler beans (legacy single-flow)
            List<StepHandler> handlers = context.getBeansOfType(StepHandler.class).values()
                    .stream().sorted(Comparator.comparingInt(StepHandler::getOrder))
                    .collect(Collectors.toList());
            if (!handlers.isEmpty()) {
                log.info("No @Flow classes found, using {} StepHandler beans", handlers.size());
                FlowTypeDescriptor desc = buildDescriptor(
                        "default", Object.class, Object.class,
                        "", handlers, props, mongoTemplate, outboxRepository,
                        stepLogRepository, objectMapper, kafkaTemplate, transactionTemplate);
                return new FlowTypeRegistry(List.of(desc));
            }
            throw new IllegalStateException(
                    "No @Flow classes or StepHandler beans found. " +
                    "Define at least one @Flow class extending FlowDefinition<YourEntity>.");
        }

        List<FlowTypeDescriptor> descriptors = new ArrayList<>();
        for (var entry : flowTypes.entrySet()) {
            var info = entry.getValue();
            FlowTypeDescriptor desc = buildDescriptor(
                    info.flowType(), info.entityClass(), info.flowDefinitionClass(),
                    info.annotatedTopic(), info.handlers(),
                    props, mongoTemplate, outboxRepository, stepLogRepository,
                    objectMapper, kafkaTemplate, transactionTemplate);
            descriptors.add(desc);
        }

        FlowTypeRegistry registry = new FlowTypeRegistry(descriptors);
        log.info("Multi-flow registry: {} flow type(s): {}",
                registry.size(), registry.getFlowTypeNames());
        return registry;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FlowTypeDescriptor buildDescriptor(
            String flowType, Class<?> entityClass, Class<?> flowDefClass,
            String annotatedTopic, List<StepHandler> handlers,
            OrchestratorProperties props, MongoTemplate mongoTemplate,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper, KafkaTemplate kafkaTemplate,
            TransactionTemplate transactionTemplate) {

        // Resolve topics: @Flow(topic=...) > YAML flows.{name}.topic > global
        OrchestratorProperties.FlowConfig flowConfig = props.getFlows().get(flowType);

        String commandTopic;
        if (annotatedTopic != null && !annotatedTopic.isEmpty()) {
            commandTopic = annotatedTopic;
        } else if (flowConfig != null && flowConfig.getTopic() != null) {
            commandTopic = flowConfig.getTopic();
        } else {
            commandTopic = props.getKafka().getCommandTopic();
        }

        String replyTopic;
        if (flowConfig != null && flowConfig.getReplyTopic() != null) {
            replyTopic = flowConfig.getReplyTopic();
        } else {
            replyTopic = props.getKafka().isReplyEnabled() ? commandTopic + ".replies" : "";
        }

        String dltTopic;
        if (flowConfig != null && flowConfig.getDltTopic() != null) {
            dltTopic = flowConfig.getDltTopic();
        } else {
            dltTopic = commandTopic + "-dlt";
        }

        boolean replyEnabled = props.getKafka().isReplyEnabled() &&
                (replyTopic != null && !replyTopic.isEmpty());

        OrchestratorProperties.RetryConfig retryConfig =
                (flowConfig != null && flowConfig.getRetry() != null)
                        ? flowConfig.getRetry() : props.getRetry();

        // Per-flow StepRegistry
        StepRegistry stepRegistry = new StepRegistry<>(handlers);

        // Per-flow Repository — auto-generate
        OrchestratorFlowRepository repository = null;
        if (entityClass != null && entityClass != Object.class) {
            repository = new GenericFlowRepository(mongoTemplate, entityClass);
            log.info("Auto-generated repository for flow '{}' entity: {}", flowType, entityClass.getSimpleName());
        }

        // Per-flow FlowOrchestrator
        FlowOrchestrator orchestrator = new FlowOrchestrator(
                repository, stepRegistry, outboxRepository, stepLogRepository,
                objectMapper, flowType, commandTopic, replyTopic, replyEnabled,
                transactionTemplate, props.getAudit().isIncludeFlowState(), kafkaTemplate);
        if (entityClass != null && entityClass != Object.class) {
            orchestrator.setEntityClass(entityClass);
        }
        orchestrator.setMongoTemplate(mongoTemplate);

        log.info("Flow '{}': topic={}, reply={}, dlt={}, steps={}, entity={}",
                flowType, commandTopic,
                replyEnabled ? replyTopic : "inline",
                dltTopic,
                stepRegistry.getStepNames(),
                entityClass != null ? entityClass.getSimpleName() : "?");

        return FlowTypeDescriptor.builder()
                .flowType(flowType)
                .entityClass(entityClass)
                .flowDefinitionClass(flowDefClass)
                .commandTopic(commandTopic)
                .replyTopic(replyTopic)
                .dltTopic(dltTopic)
                .replyEnabled(replyEnabled)
                .retryConfig(retryConfig)
                .stepRegistry(stepRegistry)
                .repository(repository)
                .orchestrator(orchestrator)
                .build();
    }

    // ========== Kafka Consumer (routes by flowType) ==========

    @Bean
    @ConditionalOnMissingBean
    public OrchestratorKafkaConsumer<?> orchestratorKafkaConsumer(
            FlowTypeRegistry registry,
            IdempotencyService idempotencyService,
            ObjectMapper objectMapper,
            OrchestratorProperties props) {
        return new OrchestratorKafkaConsumer<>(registry, idempotencyService, objectMapper,
                props.getKafka().isReplyEnabled());
    }

    // ========== Topic creation for reply topics ==========

    @Bean
    public org.apache.kafka.clients.admin.NewTopic orchestratorReplyTopic(OrchestratorProperties props) {
        if (!props.getKafka().isReplyEnabled()) return null;
        return org.springframework.kafka.config.TopicBuilder.name(props.getKafka().getReplyTopic())
                .partitions(1).build(); // uses broker default replication factor
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic orchestratorReplyDltTopic(OrchestratorProperties props) {
        if (!props.getKafka().isReplyEnabled()) return null;
        return org.springframework.kafka.config.TopicBuilder.name(props.getKafka().getReplyTopic() + "-dlt")
                .partitions(1).build(); // uses broker default replication factor
    }

    // ========== Programmatic Kafka Listeners ==========

    @Bean
    public List<ConcurrentMessageListenerContainer<String, String>> orchestratorListenerContainers(
            FlowTypeRegistry registry,
            OrchestratorKafkaConsumer<?> consumer,
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            KafkaTemplate kafkaTemplate,
            OrchestratorProperties props,
            org.springframework.core.env.Environment env) {

        List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
        String appName = env.getProperty("spring.application.name", "orchestrator");

        // Command listeners — use @KafkaListener via adapter bean (supports RetryTopicConfiguration)
        // Programmatic containers here are ONLY for reply and DLT topics.
        // Command topic listeners are registered via OrchestratorKafkaListenerAdapter below.

        // Reply listeners — one per unique reply topic (if reply mode enabled)
        for (String topic : registry.getAllReplyTopics()) {
            var container = containerFactory.createContainer(topic);
            container.getContainerProperties().setGroupId(appName + "-orchestrator");
            container.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) record -> consumer.onStepReply(
                            record.value(), record.topic(), record.offset()));
            container.setBeanName("orchestrator-reply-" + topic.replace(".", "-"));
            container.start();
            containers.add(container);
            log.info("Kafka listener: reply topic '{}' → group '{}-orchestrator'", topic, appName);
        }

        // Reply DLT listeners only — command DLT handled by @KafkaListener above
        Set<String> dltTopics = new LinkedHashSet<>();
        registry.getAll().stream()
                .filter(FlowTypeDescriptor::isReplyEnabled)
                .forEach(d -> dltTopics.add(d.getReplyTopic() + "-dlt"));

        for (String topic : dltTopics) {
            var container = containerFactory.createContainer(topic);
            container.getContainerProperties().setGroupId(appName + "-dlt");
            container.getContainerProperties().setMessageListener(
                    (MessageListener<String, String>) record -> {
                        // Extract exception from headers if available
                        String exMsg = extractDltException(record);
                        consumer.onDlt(record.value(), record.topic(), record.offset(),
                                exMsg != null ? exMsg : "unknown");
                    });
            container.setBeanName("orchestrator-dlt-" + topic.replace(".", "-"));
            container.start();
            containers.add(container);
            log.info("Kafka listener: DLT topic '{}' → group '{}-dlt'", topic, appName);
        }

        log.info("Registered {} Kafka listener containers", containers.size());
        return containers;
    }

    private String extractDltException(ConsumerRecord<String, String> record) {
        if (record.headers() == null) return null;
        var header = record.headers().lastHeader("kafka_dlt-exception-message");
        return header != null ? new String(header.value()) : null;
    }

    // ========== Command topic @KafkaListener (non-blocking retry topics) ==========

    @Bean
    public OrchestratorCommandListener orchestratorCommandListener(OrchestratorKafkaConsumer<?> consumer) {
        return new OrchestratorCommandListener(consumer);
    }

    /** Command topic listener using @KafkaListener — supports RetryTopicConfiguration
     *  for non-blocking retry topics. Reply topics use programmatic containers above. */
    public static class OrchestratorCommandListener {
        private final OrchestratorKafkaConsumer<?> consumer;
        public OrchestratorCommandListener(OrchestratorKafkaConsumer<?> consumer) { this.consumer = consumer; }

        @org.springframework.kafka.annotation.KafkaListener(
                topics = "${orchestrator.kafka.command-topic}",
                groupId = "${spring.application.name:orchestrator}-executor")
        public void onCommand(String payload,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.OFFSET) long offset) {
            consumer.onStepCommand(payload, topic, offset);
        }

        @org.springframework.kafka.annotation.KafkaListener(
                topics = "${orchestrator.kafka.command-topic}-dlt",
                groupId = "${spring.application.name:orchestrator}-dlt")
        public void onCommandDlt(String payload,
                                 @org.springframework.messaging.handler.annotation.Header(
                                         name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @org.springframework.messaging.handler.annotation.Header(
                                         name = org.springframework.kafka.support.KafkaHeaders.OFFSET) long offset,
                                 @org.springframework.messaging.handler.annotation.Header(
                                         name = "kafka_dlt-exception-message", required = false) String exceptionMessage) {
            consumer.onDlt(payload, topic, offset, exceptionMessage != null ? exceptionMessage : "unknown");
        }
    }

    @Bean
    public RetryTopicConfiguration orchestratorCommandRetryConfig(
            KafkaTemplate template,
            OrchestratorProperties props) {
        OrchestratorProperties.RetryConfig retry = props.getRetry();
        log.info("Non-blocking retry: topic={}, attempts={}, delay={}ms, jitter={}",
                props.getKafka().getCommandTopic(), retry.getMaxAttempts(),
                retry.getInitialIntervalMs(), retry.getJitterFactor());

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

    // ========== Recovery Service ==========

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    public StaleFlowRecoveryService<?> orchestratorRecoveryService(
            FlowTypeRegistry registry,
            KafkaTemplate kafkaTemplate,
            ObjectMapper objectMapper,
            OrchestratorProperties props) {
        // Use first flow type's repo/topic for backward compat
        // TODO: iterate all flow types in recovery scan
        FlowTypeDescriptor first = registry.getAll().iterator().next();
        return new StaleFlowRecoveryService(
                first.getRepository(), kafkaTemplate, objectMapper,
                first.getCommandTopic(),
                props.getRecovery().getStaleThresholdMinutes());
    }

    // ========== Backward-compatible singleton beans ==========
    // These delegate to FlowTypeRegistry for apps that inject FlowOrchestrator directly

    @Bean
    @ConditionalOnMissingBean
    public FlowOrchestrator<?> orchestratorFlowOrchestrator(FlowTypeRegistry registry) {
        return registry.getAll().iterator().next().getOrchestrator();
    }

    @Bean("orchestratorGenericFlowRepository")
    @org.springframework.context.annotation.Lazy
    @ConditionalOnMissingBean(OrchestratorFlowRepository.class)
    public OrchestratorFlowRepository<?> orchestratorGenericFlowRepository(
            @org.springframework.context.annotation.Lazy FlowTypeRegistry registry) {
        return registry.getAll().iterator().next().getRepository();
    }

    @Bean
    @ConditionalOnMissingBean
    public StepRegistry<?> orchestratorStepRegistry(FlowTypeRegistry registry) {
        return registry.getAll().iterator().next().getStepRegistry();
    }
}
