package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.GenericFlowRepository;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.*;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import com.orchestrator.starter.kafka.MongoOffsetRecoveryListener;
import com.orchestrator.starter.kafka.MongoOffsetStore;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.orchestrator.starter.kafka.TimestampOffsetRecoveryListener;
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
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
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
    public OrchestratorMetrics orchestratorMetrics(
            org.springframework.beans.factory.ObjectProvider<io.micrometer.core.instrument.MeterRegistry> meterRegistryProvider) {
        return new OrchestratorMetrics(meterRegistryProvider.getIfAvailable());
    }

    @Bean
    @ConditionalOnMissingBean
    public IdempotencyService orchestratorIdempotencyService(
            ProcessedEventRepository repository, OrchestratorMetrics metrics) {
        return new IdempotencyService(repository, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    public TopicValidator orchestratorTopicValidator(KafkaAdmin kafkaAdmin, OrchestratorProperties props) {
        return new TopicValidator(kafkaAdmin, props);
    }

    /** Index creation — skipped if Mongock is present (Mongock handles migrations) */
    @Bean
    @ConditionalOnMissingBean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass("io.mongock.runner.springboot.EnableMongock")
    public IndexInitializer orchestratorIndexInitializer(MongoTemplate mongoTemplate, OrchestratorProperties props,
                                                          @Autowired(required = false)
                                                          com.orchestrator.starter.flow.FlowTypeRegistry flowTypeRegistry) {
        return new IndexInitializer(mongoTemplate, props, flowTypeRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisher orchestratorOutboxPublisher(
            OutboxEventRepository outboxRepository,
            KafkaTemplate kafkaTemplate,
            OrchestratorProperties props,
            OrchestratorMetrics metrics) {
        return new OutboxPublisher(outboxRepository, kafkaTemplate,
                props.getOutbox().getMaxPublishRetries(), props.getOutbox().getBatchSize(), metrics);
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
            OrchestratorMetrics metrics,
            @Autowired(required = false) TransactionTemplate transactionTemplate,
            @Autowired(required = false) jakarta.validation.Validator validator) {

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
                        FlowOrchestrator.DEFAULT_FLOW_TYPE, Object.class, Object.class,
                        "", handlers, props, mongoTemplate, outboxRepository,
                        stepLogRepository, objectMapper, kafkaTemplate, transactionTemplate, metrics);
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
                    objectMapper, kafkaTemplate, transactionTemplate, metrics);
            // Wire signal registry
            desc.setSignalRegistry(info.signalRegistry());
            if (desc.getOrchestrator() != null) {
                ((FlowOrchestrator) desc.getOrchestrator()).setSignalRegistry(info.signalRegistry());
            }
            descriptors.add(desc);
        }

        FlowTypeRegistry registry = new FlowTypeRegistry(descriptors);

        // Wire registry + validator into all orchestrators
        registry.getAll().forEach(d -> {
            if (d.getOrchestrator() != null) {
                ((FlowOrchestrator) d.getOrchestrator()).setFlowTypeRegistry(registry);
                if (validator != null) {
                    ((FlowOrchestrator) d.getOrchestrator()).setValidator(validator);
                }
            }
        });

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
            TransactionTemplate transactionTemplate,
            OrchestratorMetrics metrics) {

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

        // Per-flow StepRegistry
        StepRegistry stepRegistry = new StepRegistry<>(handlers);

        // Per-flow Repository — auto-generate
        OrchestratorFlowRepository repository = null;
        if (entityClass != null && entityClass != Object.class) {
            repository = new GenericFlowRepository(mongoTemplate, entityClass);
            log.info("Auto-generated repository for flow '{}' entity: {}", flowType, entityClass.getSimpleName());
        }

        // Per-flow FlowOrchestrator
        FlowOrchestrator orchestrator = FlowOrchestrator.builder()
                .flowRepository(repository)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepository)
                .stepLogRepository(stepLogRepository)
                .objectMapper(objectMapper)
                .flowType(flowType)
                .commandTopic(commandTopic)
                .replyTopic(replyTopic)
                .replyEnabled(replyEnabled)
                .txTemplate(transactionTemplate)
                .includeFlowStateInLogs(props.getAudit().isIncludeFlowState())
                .kafkaTemplate(kafkaTemplate)
                .stepTimeoutSeconds(resolveStepTimeoutSeconds(props, flowType))
                .metrics(metrics)
                .build();
        if (entityClass != null && entityClass != Object.class) {
            orchestrator.setEntityClass(entityClass);
        }
        orchestrator.setMongoTemplate(mongoTemplate);
        orchestrator.setMaxLogSnapshotBytes(props.getAudit().getMaxLogSnapshotBytes());
        orchestrator.setDcId(props.getKafka().getClusterId());
        String hostname = System.getenv("HOSTNAME");
        orchestrator.setPodId(hostname != null && !hostname.isBlank()
                ? hostname : "pod-" + Integer.toHexString(System.identityHashCode(orchestrator)));
        // Jittered retry backoff for the WAITING_RETRY path (used in failover mode)
        var retryCfg = props.getRetry();
        orchestrator.setRetryInitialIntervalMs(retryCfg.getInitialIntervalMs());
        orchestrator.setRetryMultiplier(retryCfg.getMultiplier());
        orchestrator.setRetryMaxIntervalMs(retryCfg.getMaxIntervalMs());
        orchestrator.setRetryJitterFactor(retryCfg.getJitterFactor());
        // Validator is set separately after bean creation (injected via ObjectProvider in registry bean)
        // See orchestratorFlowTypeRegistry() for wiring

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
                .partitions(props.getKafka().getPartitions()).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic orchestratorReplyDltTopic(OrchestratorProperties props) {
        if (!props.getKafka().isReplyEnabled()) return null;
        return org.springframework.kafka.config.TopicBuilder.name(props.getKafka().getReplyTopic() + "-dlt")
                .partitions(1).build(); // DLT is low-volume, 1 partition is fine
    }

    /** Command topic — created explicitly with configured partitions.
     *  RetryTopicConfiguration creates retry/DLT topics automatically.
     *
     *  IDENTITY mode auto-scales: multiplies partitions by DC count so the single
     *  topic has the same total partition assignments as PREFIXED mode (which subscribes
     *  to N topics × partitions). Without this, IDENTITY has 1/N the consumer parallelism. */
    @Bean
    public org.apache.kafka.clients.admin.NewTopic orchestratorCommandTopic(OrchestratorProperties props) {
        return org.springframework.kafka.config.TopicBuilder.name(props.getKafka().getCommandTopic())
                .partitions(props.getKafka().getPartitions()).build();
    }

    /** DLT topic — short retention since dead-lettered flows are already marked in MongoDB.
     *  Prevents DLT accumulation across test runs and deployments. */
    @Bean
    public org.apache.kafka.clients.admin.NewTopic orchestratorCommandDltTopic(OrchestratorProperties props) {
        long retentionMs = props.getRetention().getDltRetentionHours() * 3600_000L;
        return org.springframework.kafka.config.TopicBuilder.name(props.getKafka().getCommandTopic() + "-dlt")
                .partitions(props.getKafka().getPartitions())
                .config(org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public MongoOffsetStore mongoOffsetStore(MongoTemplate mongoTemplate, OrchestratorProperties props) {
        String clusterId = props.getKafka().getClusterId();
        return new MongoOffsetStore(mongoTemplate, clusterId);
    }

    @Bean
    @ConditionalOnMissingBean
    public TimestampOffsetRecoveryListener timestampOffsetRecoveryListener(OrchestratorProperties props) {
        return new TimestampOffsetRecoveryListener(props.getRecovery());
    }

    // Note: @KafkaListener command topic containers use Spring Kafka's RetryTopicConfiguration
    // which manages its own offsets. MongoDB offset store + recovery listener is applied
    // to programmatic containers (reply + DLT) below.

    // ========== Programmatic Kafka Listeners ==========

    @Bean
    @SuppressWarnings("unchecked")
    public List<ConcurrentMessageListenerContainer<String, String>> orchestratorListenerContainers(
            FlowTypeRegistry registry,
            OrchestratorKafkaConsumer<?> consumer,
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            KafkaTemplate kafkaTemplate,
            OrchestratorProperties props,
            MongoOffsetStore mongoOffsetStore,
            TimestampOffsetRecoveryListener timestampFallback,
            org.springframework.core.env.Environment env,
            java.util.Optional<com.orchestrator.starter.failover.DcAwareListenerManager> listenerManagerOpt) {

        List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
        String appName = env.getProperty("spring.application.name", "orchestrator");

        // Build the appropriate rebalance listener based on offset store config
        ConsumerAwareRebalanceListener rebalanceListener;
        if (props.getRecovery().getOffsetStore() == OrchestratorProperties.OffsetStore.MONGO) {
            rebalanceListener = new MongoOffsetRecoveryListener(
                    mongoOffsetStore, appName + "-orchestrator", timestampFallback);
            log.info("Offset recovery: MongoDB-backed (cross-cluster failover safe)");
        } else {
            rebalanceListener = timestampFallback;
            log.info("Offset recovery: Kafka-only (single-cluster mode)");
        }

        // Wire rebalance listener into DcAwareListenerManager if failover is enabled
        var listenerManager = listenerManagerOpt.orElse(null);
        if (listenerManager != null) {
            listenerManager.setRebalanceListener(rebalanceListener);
            log.info("Failover: consumer containers will be created via DcAwareListenerManager");
        }

        // Reply listeners — one per unique reply topic (if reply mode enabled)
        for (String topic : registry.getAllReplyTopics()) {
            boolean saveMongo = props.getRecovery().getOffsetStore() == OrchestratorProperties.OffsetStore.MONGO;
            String groupId = appName + "-orchestrator";
            MessageListener<String, String> replyListener = (MessageListener<String, String>) record -> {
                        consumer.onStepReply(record.value(), record.topic(), record.offset());
                        // Save offset AFTER successful processing — prevents skipping on DC failover
                        if (saveMongo) {
                            try {
                                mongoOffsetStore.saveOffset(groupId, record.topic(),
                                        record.partition(), record.offset(),
                                        record.key(), record.timestamp());
                            } catch (Exception e) {
                                log.debug("[Offset] Failed to save offset for {}/{}: {}",
                                        record.topic(), record.partition(), e.getMessage());
                            }
                        }
                    };

            if (listenerManager != null) {
                // Failover mode: register blueprint, manager creates containers for both DCs
                var blueprint = com.orchestrator.starter.failover.ContainerBlueprint.builder()
                        .id("orchestrator-reply-" + topic.replace(".", "-"))
                        .originalTopic(topic)
                        .groupId(groupId)
                        .messageListener(replyListener)
                        .concurrency(1)
                        .build();
                var container = listenerManager.registerAndCreate(blueprint);
                containers.add(container);
            } else {
                // Single-cluster mode: create and start directly
                var container = containerFactory.createContainer(topic);
                container.getContainerProperties().setGroupId(groupId);
                container.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
                container.getContainerProperties().setMessageListener(replyListener);
                // Reply containers inherit concurrency from spring.kafka.listener.concurrency
                container.setBeanName("orchestrator-reply-" + topic.replace(".", "-"));
                container.setAutoStartup(false); // started by SmartInitializingSingleton
                containers.add(container);
            }
            log.info("Kafka listener: reply topic '{}' → group '{}-orchestrator'", topic, appName);
        }

        // Reply DLT listeners only — command DLT handled by @KafkaListener above
        Set<String> dltTopics = new LinkedHashSet<>();
        registry.getAll().stream()
                .filter(FlowTypeDescriptor::isReplyEnabled)
                .forEach(d -> dltTopics.add(d.getReplyTopic() + "-dlt"));

        for (String topic : dltTopics) {
            String dltGroupId = appName + "-dlt";
            MessageListener<String, String> dltListener = record -> {
                String exMsg = extractDltException(record);
                consumer.onDlt(record.value(), record.topic(), record.offset(),
                        exMsg != null ? exMsg : "unknown");
            };

            if (listenerManager != null) {
                var blueprint = com.orchestrator.starter.failover.ContainerBlueprint.builder()
                        .id("orchestrator-dlt-" + topic.replace(".", "-"))
                        .originalTopic(topic)
                        .groupId(dltGroupId)
                        .messageListener(dltListener)
                        .concurrency(1)
                        .build();
                var container = listenerManager.registerAndCreate(blueprint);
                containers.add(container);
            } else {
                var container = containerFactory.createContainer(topic);
                container.getContainerProperties().setGroupId(dltGroupId);
                container.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
                container.getContainerProperties().setMessageListener(dltListener);
                container.setBeanName("orchestrator-dlt-" + topic.replace(".", "-"));
                container.setAutoStartup(false); // started by SmartInitializingSingleton
                containers.add(container);
            }
            log.info("Kafka listener: DLT topic '{}' → group '{}-dlt'", topic, appName);
        }

        log.info("Registered {} Kafka listener containers", containers.size());
        return containers;
    }

    /**
     * Startup safety: validates all required beans are ready, then starts programmatic
     * Kafka containers (reply + DLT). Containers are created with autoStartup=false
     * to prevent the race condition where reply consumers process messages before
     * MongoTemplate, FlowTypeRegistry, etc. are fully initialized.
     */
    @Bean
    public org.springframework.beans.factory.SmartInitializingSingleton
    orchestratorStartupValidator(org.springframework.context.ApplicationContext context,
                                 List<ConcurrentMessageListenerContainer<String, String>> listenerContainers) {
        return () -> {
            // Resolve lazily — avoids circular dependency during bean creation
            FlowTypeRegistry registry = context.getBean(FlowTypeRegistry.class);
            MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class);
            if (registry.getAll().isEmpty()) {
                log.warn("[Startup] No flow types registered — orchestrator has nothing to process");
            }
            try {
                mongoTemplate.getDb().getName();
                log.info("[Startup] MongoDB connection verified: {}", mongoTemplate.getDb().getName());
            } catch (Exception e) {
                log.error("[Startup] MongoDB NOT reachable — flows will fail until connection is established: {}",
                        e.getMessage());
            }

            // Start programmatic containers now that context is fully refreshed
            for (var container : listenerContainers) {
                if (!container.isRunning()) {
                    container.start();
                    log.debug("[Startup] Started container: {}", container.getBeanName());
                }
            }

            log.info("[Startup] Orchestrator ready — {} flow type(s), {} Kafka containers started",
                    registry.getAll().size(), listenerContainers.size());
        };
    }

    private String extractDltException(ConsumerRecord<String, String> record) {
        if (record.headers() == null) return null;
        var header = record.headers().lastHeader("kafka_dlt-exception-message");
        return header != null ? new String(header.value()) : null;
    }

    // ========== Command topic @KafkaListener (non-blocking retry topics) ==========

    /** All configured command topics: global + per-flow overrides (no lane logic, no prefixes). */
    static Set<String> allBaseCommandTopics(OrchestratorProperties props) {
        Set<String> baseTopics = new LinkedHashSet<>();
        baseTopics.add(props.getKafka().getCommandTopic());
        props.getFlows().values().forEach(fc -> {
            if (fc.getTopic() != null && !fc.getTopic().isEmpty()) {
                baseTopics.add(fc.getTopic());
            }
        });
        return baseTopics;
    }

    /** Expand topics with {sourceAlias}.{topic} variants when PREFIXED failover is enabled. */
    static Set<String> expandWithPrefixVariants(Collection<String> baseTopics, OrchestratorProperties props) {
        var topics = new LinkedHashSet<>(baseTopics);
        var failover = props.getFailover();
        if (failover.isEnabled()
                && failover.getReplicationPolicy() == OrchestratorProperties.ReplicationPolicy.PREFIXED) {
            for (String topic : baseTopics) {
                failover.getDcs().values().forEach(dc -> {
                    if (dc.getSourceAlias() != null) {
                        topics.add(dc.getSourceAlias() + "." + topic);
                    }
                });
            }
        }
        return topics;
    }

    /** Base topics claimed by lanes, validated: no topic may belong to two lanes, no empty lane. */
    static Set<String> laneClaimedTopics(OrchestratorProperties props) {
        Set<String> claimed = new LinkedHashSet<>();
        props.getLanes().forEach((lane, cfg) -> {
            if (cfg.getTopics() == null || cfg.getTopics().isEmpty()) {
                throw new IllegalStateException("orchestrator.lanes." + lane + ": topics must not be empty");
            }
            for (String topic : cfg.getTopics()) {
                if (!claimed.add(topic)) {
                    throw new IllegalStateException("orchestrator.lanes." + lane + ": topic '" + topic
                            + "' is already claimed by another lane — a topic may belong to only one lane");
                }
            }
        });
        return claimed;
    }

    /**
     * Command topics for the DEFAULT listener: all command topics MINUS topics claimed by lanes
     * (each lane gets its own dedicated listener container — see orchestratorLaneRegistrar).
     * Single-cluster: base topics. Multi-DC PREFIXED: base + prefixed variants.
     */
    @Bean
    public String[] orchestratorCommandTopics(OrchestratorProperties props) {
        Set<String> baseTopics = allBaseCommandTopics(props);
        baseTopics.removeAll(laneClaimedTopics(props));
        Set<String> topics = expandWithPrefixVariants(baseTopics, props);
        log.info("Default-lane command topics: {} (lanes: {})", topics, props.getLanes().keySet());
        return topics.toArray(new String[0]);
    }

    @Bean
    public String[] orchestratorCommandDltTopics(OrchestratorProperties props) {
        // Collect DLT topics: per-flow overrides + derived from command topics.
        // Lane topics are included too (a lane topic that isn't any flow's topic still
        // dead-letters to {topic}-dlt and must be consumed for markDeadLettered).
        Set<String> baseDltTopics = new LinkedHashSet<>();
        baseDltTopics.add(props.getKafka().getCommandTopic() + "-dlt");
        props.getFlows().forEach((name, fc) -> {
            if (fc.getDltTopic() != null && !fc.getDltTopic().isEmpty()) {
                baseDltTopics.add(fc.getDltTopic());
            } else if (fc.getTopic() != null && !fc.getTopic().isEmpty()) {
                baseDltTopics.add(fc.getTopic() + "-dlt");
            }
        });
        props.getLanes().values().forEach(lane ->
                lane.getTopics().forEach(topic -> baseDltTopics.add(topic + "-dlt")));

        return expandWithPrefixVariants(baseDltTopics, props).toArray(new String[0]);
    }

    /**
     * Registers command listener beans from lane configuration (BeanDefinitionRegistryPostProcessor
     * because the number of beans depends on properties):
     * - the DEFAULT listener ({app}-executor) for all command topics not claimed by a lane —
     *   skipped entirely when lanes claim every topic;
     * - one {@link LaneCommandListener} per orchestrator.lanes.{lane} with its own consumer group
     *   ({app}-executor-{lane}), its own topics, and its own concurrency.
     *
     * All are real @KafkaListener beans, so RetryTopicConfiguration (non-blocking retry topics) and
     * the @Primary DcAware consumer factory (failover) apply to every lane exactly as they do to the
     * default listener. DcAwareKafkaManager stops/starts ALL registry containers on DC switch.
     */
    @Bean
    public static org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor
    orchestratorLaneRegistrar(org.springframework.core.env.Environment env) {
        return registry -> {
            OrchestratorProperties props = org.springframework.boot.context.properties.bind.Binder.get(env)
                    .bind("orchestrator", OrchestratorProperties.class)
                    .orElseGet(OrchestratorProperties::new);
            String appName = env.getProperty("spring.application.name", "orchestrator");
            boolean saveMongo = props.getRecovery().getOffsetStore() == OrchestratorProperties.OffsetStore.MONGO;

            Set<String> claimed = laneClaimedTopics(props); // validates lane config (fail fast)

            // Default listener — only when at least one command topic remains unclaimed
            Set<String> defaultTopics = allBaseCommandTopics(props);
            defaultTopics.removeAll(claimed);
            if (defaultTopics.isEmpty()) {
                log.info("All command topics are claimed by lanes {} — default command listener disabled",
                        props.getLanes().keySet());
            } else {
                var def = new org.springframework.beans.factory.support.GenericBeanDefinition();
                def.setBeanClass(OrchestratorCommandListener.class);
                def.setAutowireMode(org.springframework.beans.factory.support.AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
                def.getConstructorArgumentValues().addIndexedArgumentValue(2, saveMongo);
                def.getConstructorArgumentValues().addIndexedArgumentValue(3, appName + "-executor");
                registry.registerBeanDefinition("orchestratorCommandListener", def);
            }

            // One listener bean per lane
            props.getLanes().forEach((lane, cfg) -> {
                String[] laneTopics = expandWithPrefixVariants(cfg.getTopics(), props).toArray(new String[0]);
                String groupId = appName + "-executor-" + lane;
                var def = new org.springframework.beans.factory.support.GenericBeanDefinition();
                def.setBeanClass(LaneCommandListener.class);
                def.setAutowireMode(org.springframework.beans.factory.support.AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
                def.getConstructorArgumentValues().addIndexedArgumentValue(2, saveMongo);
                def.getConstructorArgumentValues().addIndexedArgumentValue(3, groupId);
                def.getConstructorArgumentValues().addIndexedArgumentValue(4, laneTopics);
                def.getConstructorArgumentValues().addIndexedArgumentValue(5, String.valueOf(cfg.getConcurrency()));
                def.getConstructorArgumentValues().addIndexedArgumentValue(6, lane);
                registry.registerBeanDefinition("orchestratorLaneListener_" + lane, def);
                log.info("Lane '{}': topics={} group={} concurrency={}", lane, List.of(laneTopics),
                        groupId, cfg.getConcurrency());
            });
        };
    }

    /** Shared command dispatch for the default and lane listeners. */
    static void dispatchCommand(OrchestratorKafkaConsumer<?> consumer, MongoOffsetStore mongoOffsetStore,
                                boolean saveMongo, String groupId, String payload, String topic,
                                long offset, Integer partition, Long timestamp, String key) {
        consumer.onStepCommand(payload, topic, offset);
        // Save offset AFTER successful processing — prevents message loss on DC failover.
        // If step throws (goes to retry topic), offset is NOT saved → on failover,
        // the message is re-delivered and idempotency handles the duplicate.
        if (saveMongo && partition != null && timestamp != null) {
            try {
                mongoOffsetStore.saveOffset(groupId, topic, partition, offset, key, timestamp);
            } catch (Exception e) {
                // Don't block processing if offset save fails
            }
        }
    }

    /** Default-lane command listener using @KafkaListener — supports RetryTopicConfiguration
     *  for non-blocking retry topics. Reply topics use programmatic containers above.
     *  Registered by {@link #orchestratorLaneRegistrar} unless lanes claim all command topics. */
    public static class OrchestratorCommandListener {
        private final OrchestratorKafkaConsumer<?> consumer;
        private final MongoOffsetStore mongoOffsetStore;
        private final boolean saveMongo;
        private final String groupId;

        public OrchestratorCommandListener(OrchestratorKafkaConsumer<?> consumer,
                                           MongoOffsetStore mongoOffsetStore,
                                           boolean saveMongo, String groupId) {
            this.consumer = consumer;
            this.mongoOffsetStore = mongoOffsetStore;
            this.saveMongo = saveMongo;
            this.groupId = groupId;
        }

        @org.springframework.kafka.annotation.KafkaListener(
                topics = "#{@orchestratorCommandTopics}",
                groupId = "${spring.application.name:orchestrator}-executor")
        public void onCommand(String payload,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.OFFSET) long offset,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timestamp,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY, required = false) String key) {
            dispatchCommand(consumer, mongoOffsetStore, saveMongo, groupId,
                    payload, topic, offset, partition, timestamp, key);
        }
    }

    /** Per-lane command listener: identical dispatch to the default listener, but with its own
     *  consumer group, topic set, and container concurrency (resolved from the lane config via
     *  {@code #{__listener.*}} SpEL). One bean instance per configured lane. */
    public static class LaneCommandListener {
        private final OrchestratorKafkaConsumer<?> consumer;
        private final MongoOffsetStore mongoOffsetStore;
        private final boolean saveMongo;
        private final String groupId;
        private final String[] topics;
        private final String concurrency;
        private final String lane;

        public LaneCommandListener(OrchestratorKafkaConsumer<?> consumer,
                                   MongoOffsetStore mongoOffsetStore,
                                   boolean saveMongo, String groupId,
                                   String[] topics, String concurrency, String lane) {
            this.consumer = consumer;
            this.mongoOffsetStore = mongoOffsetStore;
            this.saveMongo = saveMongo;
            this.groupId = groupId;
            this.topics = topics;
            this.concurrency = concurrency;
            this.lane = lane;
        }

        public String[] getTopics() { return topics; }
        public String getGroupId() { return groupId; }
        public String getConcurrency() { return concurrency; }
        public String getLane() { return lane; }

        @org.springframework.kafka.annotation.KafkaListener(
                topics = "#{__listener.topics}",
                groupId = "#{__listener.groupId}",
                concurrency = "#{__listener.concurrency}")
        public void onCommand(String payload,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.OFFSET) long offset,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timestamp,
                              @org.springframework.messaging.handler.annotation.Header(
                                      name = org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY, required = false) String key) {
            dispatchCommand(consumer, mongoOffsetStore, saveMongo, groupId,
                    payload, topic, offset, partition, timestamp, key);
        }
    }

    @Bean
    public OrchestratorCommandDltListener orchestratorCommandDltListener(OrchestratorKafkaConsumer<?> consumer) {
        return new OrchestratorCommandDltListener(consumer);
    }

    /** Command DLT listener — always registered (independent of lane configuration) so
     *  dead-lettered commands mark their flows regardless of which lane produced them. */
    public static class OrchestratorCommandDltListener {
        private final OrchestratorKafkaConsumer<?> consumer;

        public OrchestratorCommandDltListener(OrchestratorKafkaConsumer<?> consumer) {
            this.consumer = consumer;
        }

        @org.springframework.kafka.annotation.KafkaListener(
                topics = "#{@orchestratorCommandDltTopics}",
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

        var builder = RetryTopicConfigurationBuilder
                .newInstance()
                .customBackoff(backoff)
                .maxAttempts(retry.getMaxAttempts())
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                // Create retry/DLT topics with the SAME partition count as the main command topic.
                // Otherwise the DLT under-partitions (default 1) while the main topic (and its
                // MM2-replicated PREFIXED variant) has N partitions — a non-retryable record on
                // partition >0 then can't be dead-lettered ("non-existent DLT partition"), and with
                // DltStrategy.FAIL_ON_ERROR the error handler can't seek past it → the partition
                // wedges and every flow behind it stalls (observed in PREFIXED failover).
                //
                // Replication factor -1 = BROKER DEFAULT (KIP-464), same as the TopicBuilder beans.
                // A hardcoded RF (e.g. 1) breaks multi-broker clusters whose min.insync.replicas
                // exceeds it: every retry/DLT publication is rejected with NOT_ENOUGH_REPLICAS, the
                // error handler falls back to in-place seeks, and the retry chain silently never
                // runs (observed live on a 3-broker cluster with min.insync.replicas=2).
                .autoCreateTopics(true, props.getKafka().getPartitions(), (short) -1)
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .retryOn(RetryableStepException.class);

        // Include all command topics: global + per-flow + lane topics + prefixed failover variants.
        // Lane topics get the same retry chains — each lane's @KafkaListener endpoint is decorated
        // by this configuration for the topics it consumes.
        Set<String> commandTopics = allBaseCommandTopics(props);
        props.getLanes().values().forEach(lane -> commandTopics.addAll(lane.getTopics()));
        for (String topic : expandWithPrefixVariants(commandTopics, props)) {
            builder.includeTopic(topic);
        }

        return builder.create(template);
    }

    /** Effective flow-level step timeout: orchestrator.flows.{name}.step-timeout-seconds if set,
     *  else the global orchestrator.step.timeout-seconds. Individual steps can still override
     *  via @Step(timeoutSeconds=...). */
    public static int resolveStepTimeoutSeconds(OrchestratorProperties props, String flowType) {
        OrchestratorProperties.FlowConfig fc = props.getFlows().get(flowType);
        if (fc != null && fc.getStepTimeoutSeconds() != null) {
            return fc.getStepTimeoutSeconds();
        }
        return props.getStep().getTimeoutSeconds();
    }

    // ========== Health Indicator ==========

    @Bean
    @ConditionalOnMissingBean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnClass(
            name = "org.springframework.boot.health.contributor.HealthIndicator")
    public OrchestratorHealthIndicator orchestratorHealthIndicator(
            OutboxEventRepository outboxRepository,
            FlowTypeRegistry registry,
            OrchestratorProperties props) {
        return new OrchestratorHealthIndicator(outboxRepository, registry,
                props.getHealth().getOutboxThreshold(),
                props.getRecovery().getStaleThresholdMinutes());
    }

    // ========== Recovery Service ==========

    @Bean
    @ConditionalOnMissingBean
    public StaleFlowRecoveryService orchestratorRecoveryService(
            FlowTypeRegistry registry,
            KafkaTemplate kafkaTemplate,
            ObjectMapper objectMapper,
            MongoTemplate mongoTemplate,
            OrchestratorProperties props,
            OutboxEventRepository outboxRepository,
            OrchestratorMetrics metrics) {
        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, objectMapper, mongoTemplate,
                props.getRecovery().getStaleThresholdMinutes(),
                props.getRecovery().getMaxRecoveryAttempts(),
                props.getRecovery().getBatchSize(),
                props.getRecovery().getClaimTtlMinutes(),
                outboxRepository, metrics);
        service.setExecutionClaimTtlMinutes(props.getRecovery().getExecutionClaimTtlMinutes());
        return service;
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

    /** Shuts down orchestrator executors on application stop. */
    @Bean
    @SuppressWarnings("rawtypes")
    public org.springframework.context.ApplicationListener<org.springframework.context.event.ContextClosedEvent>
            orchestratorShutdownHook(FlowTypeRegistry registry) {
        return event -> {
            registry.getAll().forEach(d -> {
                if (d.getOrchestrator() != null) {
                    ((FlowOrchestrator) d.getOrchestrator()).shutdown();
                }
            });
            log.info("Orchestrator executors shut down");
        };
    }
}
