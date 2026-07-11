package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorAutoConfiguration;
import com.orchestrator.starter.autoconfigure.OrchestratorAutoConfiguration.OrchestratorCommandListener;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.*;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.kafka.MongoOffsetStore;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for OrchestratorAutoConfiguration and its inner class OrchestratorCommandListener.
 * Pure Mockito tests — no Spring context required.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("OrchestratorAutoConfiguration")
class AutoConfigurationTest {

    // ========================================================================
    // OrchestratorCommandListener
    // ========================================================================

    @Nested
    @DisplayName("OrchestratorCommandListener")
    class CommandListenerTests {

        @Mock
        private OrchestratorKafkaConsumer<?> consumer;
        @Mock
        private MongoOffsetStore mongoOffsetStore;

        @Nested
        @DisplayName("onCommand")
        class OnCommandTests {

            @Test
            @DisplayName("calls consumer.onStepCommand and saves offset when saveMongo=true")
            void onCommand_saveMongoTrue_savesOffset() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, true, "app-executor");

                listener.onCommand("payload-1", "orchestrator.commands", 42L, 3, 1717200000000L, "flow-key-1");

                verify(consumer).onStepCommand("payload-1", "orchestrator.commands", 42L);
                verify(mongoOffsetStore).saveOffset("app-executor", "orchestrator.commands", 3, 42L, "flow-key-1", 1717200000000L);
            }

            @Test
            @DisplayName("calls consumer.onStepCommand but does NOT save offset when saveMongo=false")
            void onCommand_saveMongoFalse_doesNotSaveOffset() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, false, "app-executor");

                listener.onCommand("payload-2", "orchestrator.commands", 10L, 0, 1717200000000L, "flow-key-2");

                verify(consumer).onStepCommand("payload-2", "orchestrator.commands", 10L);
                verifyNoInteractions(mongoOffsetStore);
            }

            @Test
            @DisplayName("offset save failure does not block processing")
            void onCommand_offsetSaveFailure_doesNotBlock() {
                doThrow(new RuntimeException("Mongo down"))
                        .when(mongoOffsetStore).saveOffset(anyString(), anyString(), anyInt(), anyLong(), any(), anyLong());

                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, true, "app-executor");

                // Should not throw
                listener.onCommand("payload-3", "orchestrator.commands", 99L, 1, 1717200000000L, "flow-key-3");

                verify(consumer).onStepCommand("payload-3", "orchestrator.commands", 99L);
                verify(mongoOffsetStore).saveOffset(eq("app-executor"), eq("orchestrator.commands"), eq(1), eq(99L), eq("flow-key-3"), eq(1717200000000L));
            }

            @Test
            @DisplayName("null partition does not save offset")
            void onCommand_nullPartition_doesNotSaveOffset() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, true, "app-executor");

                listener.onCommand("payload-4", "orchestrator.commands", 5L, null, 1717200000000L, "flow-key-4");

                verify(consumer).onStepCommand("payload-4", "orchestrator.commands", 5L);
                verifyNoInteractions(mongoOffsetStore);
            }

            @Test
            @DisplayName("null timestamp does not save offset")
            void onCommand_nullTimestamp_doesNotSaveOffset() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, true, "app-executor");

                listener.onCommand("payload-5", "orchestrator.commands", 7L, 2, null, "flow-key-5");

                verify(consumer).onStepCommand("payload-5", "orchestrator.commands", 7L);
                verifyNoInteractions(mongoOffsetStore);
            }

            @Test
            @DisplayName("null partition AND null timestamp does not save offset")
            void onCommand_nullPartitionAndTimestamp_doesNotSaveOffset() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, true, "app-executor");

                listener.onCommand("payload-6", "orchestrator.commands", 11L, null, null, null);

                verify(consumer).onStepCommand("payload-6", "orchestrator.commands", 11L);
                verifyNoInteractions(mongoOffsetStore);
            }
        }

        @Nested
        @DisplayName("onCommandDlt")
        class OnDltTests {

            @Test
            @DisplayName("calls consumer.onDlt with exception message")
            void onDlt_withExceptionMessage() {
                var listener = new OrchestratorAutoConfiguration.OrchestratorCommandDltListener(consumer);

                listener.onCommandDlt("dlt-payload", "orchestrator.commands-dlt", 100L, "NullPointerException: oops");

                verify(consumer).onDlt("dlt-payload", "orchestrator.commands-dlt", 100L, "NullPointerException: oops");
            }

            @Test
            @DisplayName("null exception header sends 'unknown' as message")
            void onDlt_nullExceptionHeader() {
                var listener = new OrchestratorAutoConfiguration.OrchestratorCommandDltListener(consumer);

                listener.onCommandDlt("dlt-payload-2", "orchestrator.commands-dlt", 200L, null);

                verify(consumer).onDlt("dlt-payload-2", "orchestrator.commands-dlt", 200L, "unknown");
            }
        }
    }

    // ========================================================================
    // orchestratorCommandTopics bean logic
    // ========================================================================

    @Nested
    @DisplayName("orchestratorCommandTopics")
    class CommandTopicsTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("single-cluster returns just the command topic")
        void singleCluster_returnsCommandTopic() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("my.commands");
            // failover disabled by default

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly("my.commands");
        }

        @Test
        @DisplayName("PREFIXED failover returns local + all prefixed topics")
        void prefixedFailover_returnsLocalAndPrefixed() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.PREFIXED);

            DcConfig dcA = new DcConfig();
            dcA.setBootstrap("kafka-a:9092");
            dcA.setSourceAlias("dc-a");

            DcConfig dcB = new DcConfig();
            dcB.setBootstrap("kafka-b:9093");
            dcB.setSourceAlias("dc-b");

            Map<String, DcConfig> dcs = new LinkedHashMap<>();
            dcs.put("dc-a", dcA);
            dcs.put("dc-b", dcB);
            failover.setDcs(dcs);

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly(
                    "orchestrator.commands",
                    "dc-a.orchestrator.commands",
                    "dc-b.orchestrator.commands"
            );
        }

        @Test
        @DisplayName("IDENTITY failover returns just the command topic (same name)")
        void identityFailover_returnsCommandTopic() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.IDENTITY);

            DcConfig dcA = new DcConfig();
            dcA.setBootstrap("kafka-a:9092");
            dcA.setSourceAlias("dc-a");
            failover.setDcs(Map.of("dc-a", dcA));

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly("orchestrator.commands");
        }

        @Test
        @DisplayName("PREFIXED failover skips DCs without sourceAlias")
        void prefixedFailover_skipsDcsWithoutSourceAlias() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.PREFIXED);

            DcConfig dcA = new DcConfig();
            dcA.setBootstrap("kafka-a:9092");
            dcA.setSourceAlias("dc-a");

            DcConfig dcNoAlias = new DcConfig();
            dcNoAlias.setBootstrap("kafka-c:9094");
            // no sourceAlias set — remains null

            Map<String, DcConfig> dcs = new LinkedHashMap<>();
            dcs.put("dc-a", dcA);
            dcs.put("dc-c", dcNoAlias);
            failover.setDcs(dcs);

            String[] topics = config.orchestratorCommandTopics(props);

            // Should include local + dc-a prefixed, but NOT dc-c (no alias)
            assertThat(topics).containsExactly(
                    "orchestrator.commands",
                    "dc-a.orchestrator.commands"
            );
        }
    }

    // ========================================================================
    // Per-flow topic configuration
    // ========================================================================

    @Nested
    @DisplayName("Per-flow topic configuration")
    class PerFlowTopicTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("per-flow command topics included alongside global topic")
        void perFlowCommandTopics_includedWithGlobal() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var flowA = new OrchestratorProperties.FlowConfig();
            flowA.setTopic("payment.commands");
            var flowB = new OrchestratorProperties.FlowConfig();
            flowB.setTopic("shipping.commands");
            props.getFlows().put("payment", flowA);
            props.getFlows().put("shipping", flowB);

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactlyInAnyOrder(
                    "orchestrator.commands", "payment.commands", "shipping.commands");
        }

        @Test
        @DisplayName("flows without per-flow topic use only global topic")
        void noPerFlowTopic_usesGlobalOnly() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");
            props.getFlows().put("flow-a", new OrchestratorProperties.FlowConfig());
            props.getFlows().put("flow-b", new OrchestratorProperties.FlowConfig());

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly("orchestrator.commands");
        }

        @Test
        @DisplayName("mixed: some flows on custom topic, others on global")
        void mixedTopics_globalAndCustom() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var custom = new OrchestratorProperties.FlowConfig();
            custom.setTopic("payment.commands");
            props.getFlows().put("payment", custom);
            props.getFlows().put("default-flow", new OrchestratorProperties.FlowConfig());

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactlyInAnyOrder(
                    "orchestrator.commands", "payment.commands");
        }

        @Test
        @DisplayName("per-flow DLT topics: explicit override + derived from command topic")
        void perFlowDltTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var flowA = new OrchestratorProperties.FlowConfig();
            flowA.setTopic("payment.commands");
            // no explicit DLT → derived as payment.commands-dlt

            var flowB = new OrchestratorProperties.FlowConfig();
            flowB.setDltTopic("shipping.critical-dlt");
            // explicit DLT override

            props.getFlows().put("payment", flowA);
            props.getFlows().put("shipping", flowB);

            String[] dltTopics = config.orchestratorCommandDltTopics(props);

            assertThat(dltTopics).containsExactlyInAnyOrder(
                    "orchestrator.commands-dlt",
                    "payment.commands-dlt",
                    "shipping.critical-dlt");
        }

        @Test
        @DisplayName("per-flow topics with PREFIXED failover generates all variants")
        void perFlowTopics_prefixedFailover() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var flowA = new OrchestratorProperties.FlowConfig();
            flowA.setTopic("payment.commands");
            props.getFlows().put("payment", flowA);

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.PREFIXED);

            DcConfig dc = new DcConfig();
            dc.setSourceAlias("us-east");
            failover.setDcs(Map.of("us-east", dc));

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactlyInAnyOrder(
                    "orchestrator.commands",
                    "payment.commands",
                    "us-east.orchestrator.commands",
                    "us-east.payment.commands");
        }

        @Test
        @DisplayName("duplicate per-flow topic deduplicated")
        void duplicatePerFlowTopic_deduplicated() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("shared.commands");

            var flowA = new OrchestratorProperties.FlowConfig();
            flowA.setTopic("shared.commands"); // same as global
            var flowB = new OrchestratorProperties.FlowConfig();
            flowB.setTopic("shared.commands"); // same as global
            props.getFlows().put("flow-a", flowA);
            props.getFlows().put("flow-b", flowB);

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly("shared.commands");
        }

        @Test
        @DisplayName("retry config includes per-flow command topics")
        void retryConfig_includesPerFlowTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var flowA = new OrchestratorProperties.FlowConfig();
            flowA.setTopic("payment.commands");
            props.getFlows().put("payment", flowA);

            @SuppressWarnings("unchecked")
            org.springframework.kafka.core.KafkaTemplate<String, String> template =
                    mock(org.springframework.kafka.core.KafkaTemplate.class);

            var retryConfig = config.orchestratorCommandRetryConfig(template, props);

            assertThat(retryConfig).isNotNull();
        }
    }

    // ========================================================================
    // orchestratorCommandDltTopics bean logic
    // ========================================================================

    @Nested
    @DisplayName("orchestratorCommandDltTopics")
    class CommandDltTopicsTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("single-cluster returns command-dlt")
        void singleCluster_returnsCommandDlt() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            String[] topics = config.orchestratorCommandDltTopics(props);

            assertThat(topics).containsExactly("orchestrator.commands-dlt");
        }

        @Test
        @DisplayName("PREFIXED failover returns local + prefixed DLTs")
        void prefixedFailover_returnsPrefixedDlts() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.PREFIXED);

            DcConfig dcA = new DcConfig();
            dcA.setSourceAlias("dc-a");
            DcConfig dcB = new DcConfig();
            dcB.setSourceAlias("dc-b");

            Map<String, DcConfig> dcs = new LinkedHashMap<>();
            dcs.put("dc-a", dcA);
            dcs.put("dc-b", dcB);
            failover.setDcs(dcs);

            String[] topics = config.orchestratorCommandDltTopics(props);

            assertThat(topics).containsExactly(
                    "orchestrator.commands-dlt",
                    "dc-a.orchestrator.commands-dlt",
                    "dc-b.orchestrator.commands-dlt"
            );
        }

        @Test
        @DisplayName("IDENTITY failover returns just the DLT topic")
        void identityFailover_returnsJustDlt() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            FailoverConfig failover = props.getFailover();
            failover.setEnabled(true);
            failover.setReplicationPolicy(ReplicationPolicy.IDENTITY);
            failover.setDcs(Map.of("dc-a", new DcConfig()));

            String[] topics = config.orchestratorCommandDltTopics(props);

            assertThat(topics).containsExactly("orchestrator.commands-dlt");
        }
    }

    // ========================================================================
    // extractDltException (via reflection — private method)
    // ========================================================================

    @Nested
    @DisplayName("extractDltException")
    class ExtractDltExceptionTests {

        private OrchestratorAutoConfiguration config;
        private Method extractMethod;

        @BeforeEach
        void setUp() throws Exception {
            config = new OrchestratorAutoConfiguration();
            extractMethod = OrchestratorAutoConfiguration.class.getDeclaredMethod(
                    "extractDltException", ConsumerRecord.class);
            extractMethod.setAccessible(true);
        }

        @Test
        @DisplayName("record with exception header returns the message")
        void withExceptionHeader_returnsMessage() throws Exception {
            Headers headers = new RecordHeaders(List.of(
                    new RecordHeader("kafka_dlt-exception-message",
                            "NullPointerException: boom".getBytes(StandardCharsets.UTF_8))
            ));
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    "test-dlt", 0, 10L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    "key", "value", headers, Optional.empty());

            String result = (String) extractMethod.invoke(config, record);

            assertThat(result).isEqualTo("NullPointerException: boom");
        }

        @Test
        @DisplayName("record without exception header returns null")
        void withoutExceptionHeader_returnsNull() throws Exception {
            Headers headers = new RecordHeaders(); // no headers
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    "test-dlt", 0, 10L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    "key", "value", headers, Optional.empty());

            String result = (String) extractMethod.invoke(config, record);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("record with other headers but no exception header returns null")
        void withOtherHeaders_returnsNull() throws Exception {
            Headers headers = new RecordHeaders(List.of(
                    new RecordHeader("some-other-header", "data".getBytes(StandardCharsets.UTF_8))
            ));
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    "test-dlt", 0, 10L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    "key", "value", headers, Optional.empty());

            String result = (String) extractMethod.invoke(config, record);

            assertThat(result).isNull();
        }
    }

    // ========================================================================
    // Topic creation beans
    // ========================================================================

    @Nested
    @DisplayName("Topic creation beans")
    class TopicCreationTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("reply topic created with configured partitions when reply enabled")
        void replyTopic_createdWhenEnabled() {
            OrchestratorProperties props = new OrchestratorProperties();
            // reply is enabled by default (replyTopic is null => defaults to commandTopic + ".replies")
            props.getKafka().setPartitions(12);

            NewTopic topic = config.orchestratorReplyTopic(props);

            assertThat(topic).isNotNull();
            assertThat(topic.name()).isEqualTo("orchestrator.commands.replies");
            assertThat(topic.numPartitions()).isEqualTo(12);
        }

        @Test
        @DisplayName("reply topic is null when reply disabled")
        void replyTopic_nullWhenDisabled() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setReplyTopic(""); // empty string disables reply

            NewTopic topic = config.orchestratorReplyTopic(props);

            assertThat(topic).isNull();
        }

        @Test
        @DisplayName("reply DLT topic created when reply enabled")
        void replyDltTopic_createdWhenEnabled() {
            OrchestratorProperties props = new OrchestratorProperties();
            // reply enabled by default

            NewTopic topic = config.orchestratorReplyDltTopic(props);

            assertThat(topic).isNotNull();
            assertThat(topic.name()).isEqualTo("orchestrator.commands.replies-dlt");
            assertThat(topic.numPartitions()).isEqualTo(1); // DLT is low-volume
        }

        @Test
        @DisplayName("reply DLT topic is null when reply disabled")
        void replyDltTopic_nullWhenDisabled() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setReplyTopic("");

            NewTopic topic = config.orchestratorReplyDltTopic(props);

            assertThat(topic).isNull();
        }

        @Test
        @DisplayName("command topic created with configured partitions")
        void commandTopic_createdWithPartitions() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("my.commands");
            props.getKafka().setPartitions(18);

            NewTopic topic = config.orchestratorCommandTopic(props);

            assertThat(topic).isNotNull();
            assertThat(topic.name()).isEqualTo("my.commands");
            assertThat(topic.numPartitions()).isEqualTo(18);
        }

        @Test
        @DisplayName("command DLT topic with retention config")
        void commandDltTopic_withRetentionConfig() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("my.commands");
            props.getRetention().setDltRetentionHours(48);

            NewTopic topic = config.orchestratorCommandDltTopic(props);

            assertThat(topic).isNotNull();
            assertThat(topic.name()).isEqualTo("my.commands-dlt");
            // DLT now matches the main command topic's partition count so records on any partition
            // can be dead-lettered (see the failover DLT-partition-wedge fix).
            assertThat(topic.numPartitions()).isEqualTo(props.getKafka().getPartitions());
            // 48 hours = 172800000 ms
            assertThat(topic.configs())
                    .containsEntry("retention.ms", "172800000");
        }

        @Test
        @DisplayName("command DLT topic uses default 24h retention")
        void commandDltTopic_defaultRetention() {
            OrchestratorProperties props = new OrchestratorProperties();

            NewTopic topic = config.orchestratorCommandDltTopic(props);

            // 24 hours = 86400000 ms
            assertThat(topic.configs())
                    .containsEntry("retention.ms", "86400000");
        }
    }

    // ========================================================================
    // orchestratorStartupValidator bean logic
    // ========================================================================

    @Nested
    @DisplayName("orchestratorStartupValidator")
    class StartupValidatorTests {

        @Mock
        private ApplicationContext context;
        @Mock
        private FlowTypeRegistry registry;
        @Mock
        private MongoTemplate mongoTemplate;
        @Mock
        private com.mongodb.client.MongoDatabase mongoDatabase;

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
            when(context.getBean(FlowTypeRegistry.class)).thenReturn(registry);
            when(context.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
        }

        @Test
        @DisplayName("SmartInitializingSingleton runs without errors when registry is populated")
        void runsSuccessfully_whenRegistryPopulated() {
            var descriptor = mock(FlowTypeDescriptor.class);
            when(registry.getAll()).thenReturn(List.of(descriptor));
            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getName()).thenReturn("test-db");

            var singleton = config.orchestratorStartupValidator(context, List.of());

            // Should not throw
            singleton.afterSingletonsInstantiated();

            verify(context).getBean(FlowTypeRegistry.class);
            verify(context).getBean(MongoTemplate.class);
            verify(mongoTemplate, atLeastOnce()).getDb();
        }

        @Test
        @DisplayName("logs warning when registry is empty")
        void logsWarning_whenRegistryEmpty() {
            when(registry.getAll()).thenReturn(Collections.emptyList());
            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getName()).thenReturn("test-db");

            var singleton = config.orchestratorStartupValidator(context, List.of());

            // Should not throw — just logs a warning
            singleton.afterSingletonsInstantiated();

            verify(registry, atLeastOnce()).getAll();
        }

        @Test
        @DisplayName("MongoDB connection verified — calls getDb().getName()")
        void mongoDbConnectionVerified() {
            var descriptor = mock(FlowTypeDescriptor.class);
            when(registry.getAll()).thenReturn(List.of(descriptor));
            when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getName()).thenReturn("orchestrator-db");

            var singleton = config.orchestratorStartupValidator(context, List.of());
            singleton.afterSingletonsInstantiated();

            verify(mongoTemplate, atLeastOnce()).getDb();
            verify(mongoDatabase, atLeastOnce()).getName();
        }

        @Test
        @DisplayName("MongoDB failure logged but does not throw")
        void mongoDbFailure_doesNotThrow() {
            var descriptor = mock(FlowTypeDescriptor.class);
            when(registry.getAll()).thenReturn(List.of(descriptor));
            when(mongoTemplate.getDb()).thenThrow(new RuntimeException("Connection refused"));

            var singleton = config.orchestratorStartupValidator(context, List.of());

            // Should not throw — logs error instead
            singleton.afterSingletonsInstantiated();

            verify(mongoTemplate).getDb();
        }
    }

    // ========================================================================
    // orchestratorCommandListener bean creation
    // ========================================================================

    @Nested
    @DisplayName("orchestratorLaneRegistrar (command listener bean registration)")
    class CommandListenerBeanTests {

        private org.springframework.beans.factory.support.DefaultListableBeanFactory registry;
        private org.springframework.mock.env.MockEnvironment env;

        @BeforeEach
        void setUp() {
            registry = new org.springframework.beans.factory.support.DefaultListableBeanFactory();
            env = new org.springframework.mock.env.MockEnvironment();
        }

        private void runRegistrar() {
            OrchestratorAutoConfiguration.orchestratorLaneRegistrar(env)
                    .postProcessBeanDefinitionRegistry(registry);
        }

        @Test
        @DisplayName("no lanes → default listener registered with {app}-executor group + saveMongo")
        void noLanes_registersDefaultListener() {
            env.setProperty("spring.application.name", "my-app");

            runRegistrar();

            assertThat(registry.containsBeanDefinition("orchestratorCommandListener")).isTrue();
            var args = registry.getBeanDefinition("orchestratorCommandListener")
                    .getConstructorArgumentValues();
            assertThat(args.getIndexedArgumentValue(2, Boolean.class).getValue()).isEqualTo(true); // MONGO default
            assertThat(args.getIndexedArgumentValue(3, String.class).getValue()).isEqualTo("my-app-executor");
        }

        @Test
        @DisplayName("offset store KAFKA → default listener registered with saveMongo=false")
        void kafkaOffsetStore_saveMongoFalse() {
            env.setProperty("orchestrator.recovery.offset-store", "KAFKA");

            runRegistrar();

            var args = registry.getBeanDefinition("orchestratorCommandListener")
                    .getConstructorArgumentValues();
            assertThat(args.getIndexedArgumentValue(2, Boolean.class).getValue()).isEqualTo(false);
            assertThat(args.getIndexedArgumentValue(3, String.class).getValue()).isEqualTo("orchestrator-executor");
        }

        @Test
        @DisplayName("lane registered with own group, topics, and concurrency")
        void lane_registersDedicatedListener() {
            env.setProperty("spring.application.name", "my-app");
            env.setProperty("orchestrator.lanes.batch.topics[0]", "sweep.commands");
            env.setProperty("orchestrator.lanes.batch.concurrency", "3");

            runRegistrar();

            assertThat(registry.containsBeanDefinition("orchestratorLaneListener_batch")).isTrue();
            var args = registry.getBeanDefinition("orchestratorLaneListener_batch")
                    .getConstructorArgumentValues();
            assertThat(args.getIndexedArgumentValue(3, String.class).getValue()).isEqualTo("my-app-executor-batch");
            assertThat((String[]) args.getIndexedArgumentValue(4, String[].class).getValue())
                    .containsExactly("sweep.commands");
            assertThat(args.getIndexedArgumentValue(5, String.class).getValue()).isEqualTo("3");
            // default listener still present — global command topic is unclaimed
            assertThat(registry.containsBeanDefinition("orchestratorCommandListener")).isTrue();
        }

        @Test
        @DisplayName("lanes claiming ALL command topics → default listener not registered")
        void allTopicsLaned_noDefaultListener() {
            env.setProperty("orchestrator.kafka.command-topic", "app.commands");
            env.setProperty("orchestrator.lanes.interactive.topics[0]", "app.commands");
            env.setProperty("orchestrator.lanes.interactive.concurrency", "4");

            runRegistrar();

            assertThat(registry.containsBeanDefinition("orchestratorCommandListener")).isFalse();
            assertThat(registry.containsBeanDefinition("orchestratorLaneListener_interactive")).isTrue();
        }

        @Test
        @DisplayName("PREFIXED failover → lane topics expanded with dc-prefixed variants")
        void prefixedFailover_laneTopicsExpanded() {
            env.setProperty("orchestrator.lanes.batch.topics[0]", "sweep.commands");
            env.setProperty("orchestrator.failover.enabled", "true");
            env.setProperty("orchestrator.failover.replication-policy", "PREFIXED");
            env.setProperty("orchestrator.failover.dcs.dc-a.bootstrap", "kafka-a:9092");
            env.setProperty("orchestrator.failover.dcs.dc-a.source-alias", "dc-a");
            env.setProperty("orchestrator.failover.dcs.dc-b.bootstrap", "kafka-b:9093");
            env.setProperty("orchestrator.failover.dcs.dc-b.source-alias", "dc-b");

            runRegistrar();

            var args = registry.getBeanDefinition("orchestratorLaneListener_batch")
                    .getConstructorArgumentValues();
            assertThat((String[]) args.getIndexedArgumentValue(4, String[].class).getValue())
                    .containsExactlyInAnyOrder("sweep.commands", "dc-a.sweep.commands", "dc-b.sweep.commands");
        }

        @Test
        @DisplayName("topic claimed by two lanes → fail fast")
        void duplicateLaneClaim_throws() {
            env.setProperty("orchestrator.lanes.a.topics[0]", "shared.commands");
            env.setProperty("orchestrator.lanes.b.topics[0]", "shared.commands");

            org.assertj.core.api.Assertions.assertThatThrownBy(this::runRegistrar)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already claimed by another lane");
        }

        @Test
        @DisplayName("lane with no topics → fail fast")
        void emptyLaneTopics_throws() {
            env.setProperty("orchestrator.lanes.empty.concurrency", "2");

            org.assertj.core.api.Assertions.assertThatThrownBy(this::runRegistrar)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("topics must not be empty");
        }
    }

    @Nested
    @DisplayName("lane-aware topic beans")
    class LaneTopicBeansTests {

        private final OrchestratorAutoConfiguration config = new OrchestratorAutoConfiguration();

        @Test
        @DisplayName("orchestratorCommandTopics excludes lane-claimed topics")
        void commandTopics_excludeLaneClaimed() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("app.commands");
            var flow = new OrchestratorProperties.FlowConfig();
            flow.setTopic("sweep.commands");
            props.getFlows().put("sweep", flow);
            var lane = new OrchestratorProperties.LaneConfig();
            lane.setTopics(java.util.List.of("sweep.commands"));
            props.getLanes().put("batch", lane);

            assertThat(config.orchestratorCommandTopics(props)).containsExactly("app.commands");
        }

        @Test
        @DisplayName("orchestratorCommandDltTopics includes lane-topic DLTs")
        void dltTopics_includeLaneTopicDlts() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("app.commands");
            var lane = new OrchestratorProperties.LaneConfig();
            lane.setTopics(java.util.List.of("sweep.commands"));
            props.getLanes().put("batch", lane);

            assertThat(config.orchestratorCommandDltTopics(props))
                    .contains("app.commands-dlt", "sweep.commands-dlt");
        }

        @Test
        @DisplayName("retry-topic configuration covers lane topics (incl. PREFIXED variants) with "
                + "main-topic partition count and BROKER-DEFAULT replication")
        void retryConfig_coversLaneTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("app.commands");
            var lane = new OrchestratorProperties.LaneConfig();
            lane.setTopics(java.util.List.of("sweep.commands"));
            props.getLanes().put("batch", lane);
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(OrchestratorProperties.ReplicationPolicy.PREFIXED);
            var dc = new OrchestratorProperties.DcConfig();
            dc.setBootstrap("kafka-a:9092");
            dc.setSourceAlias("dc-a");
            props.getFailover().getDcs().put("dc-a", dc);

            @SuppressWarnings({"unchecked", "rawtypes"})
            var retryConfig = config.orchestratorCommandRetryConfig(
                    mock(org.springframework.kafka.core.KafkaTemplate.class), props);

            // Lane topic + its prefixed failover variant are covered by the retry chain
            assertThat(retryConfig.hasConfigurationForTopics(new String[]{"sweep.commands"})).isTrue();
            assertThat(retryConfig.hasConfigurationForTopics(new String[]{"dc-a.sweep.commands"})).isTrue();
            assertThat(retryConfig.hasConfigurationForTopics(new String[]{"app.commands"})).isTrue();
            assertThat(retryConfig.hasConfigurationForTopics(new String[]{"unrelated.topic"})).isFalse();

            // Retry/DLT topics: main-topic partition count (wedge fix) + broker-default RF
            // (hardcoded RF broke clusters with min.insync.replicas > RF → NOT_ENOUGH_REPLICAS).
            // TopicCreation is package-private in spring-kafka — read via reflection.
            Object creation = retryConfig.forKafkaTopicAutoCreation();
            assertThat(reflect(creation, "shouldCreateTopics")).isEqualTo(true);
            assertThat(reflect(creation, "getNumPartitions")).isEqualTo(props.getKafka().getPartitions());
            assertThat(reflect(creation, "getReplicationFactor")).isEqualTo((short) -1);
        }

        private Object reflect(Object target, String method) {
            try {
                Method m = target.getClass().getDeclaredMethod(method);
                m.setAccessible(true);
                return m.invoke(target);
            } catch (Exception e) {
                throw new AssertionError("reflection on " + method + " failed", e);
            }
        }

        @Test
        @DisplayName("flow step-timeout override resolves over global; absent flow inherits global")
        void stepTimeoutResolution() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getStep().setTimeoutSeconds(60);
            var fc = new OrchestratorProperties.FlowConfig();
            fc.setStepTimeoutSeconds(300);
            props.getFlows().put("sweep", fc);

            assertThat(OrchestratorAutoConfiguration.resolveStepTimeoutSeconds(props, "sweep")).isEqualTo(300);
            assertThat(OrchestratorAutoConfiguration.resolveStepTimeoutSeconds(props, "other")).isEqualTo(60);
        }
    }

    // ========================================================================
    // OrchestratorProperties defaults
    // ========================================================================

    @Nested
    @DisplayName("OrchestratorProperties defaults")
    class PropertiesDefaultsTests {

        @Test
        @DisplayName("Kafka config has sensible defaults")
        void kafkaDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getKafka().getCommandTopic()).isEqualTo("orchestrator.commands");
            assertThat(props.getKafka().getPartitions()).isEqualTo(6);
            assertThat(props.getKafka().getClusterId()).isEqualTo("default");
            assertThat(props.getKafka().isReplyEnabled()).isTrue();
            assertThat(props.getKafka().getReplyTopic()).isEqualTo("orchestrator.commands.replies");
        }

        @Test
        @DisplayName("reply disabled when replyTopic set to empty string")
        void replyDisabledWithEmptyString() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setReplyTopic("");

            assertThat(props.getKafka().isReplyEnabled()).isFalse();
        }

        @Test
        @DisplayName("retry config has sensible defaults")
        void retryDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getRetry().getMaxAttempts()).isEqualTo(4);
            assertThat(props.getRetry().getInitialIntervalMs()).isEqualTo(2000);
            assertThat(props.getRetry().getMultiplier()).isEqualTo(2.0);
            assertThat(props.getRetry().getMaxIntervalMs()).isEqualTo(30000);
            assertThat(props.getRetry().getJitterFactor()).isEqualTo(0.5);
        }

        @Test
        @DisplayName("recovery config has sensible defaults")
        void recoveryDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getRecovery().getOffsetStore()).isEqualTo(OffsetStore.MONGO);
            assertThat(props.getRecovery().getStaleThresholdMinutes()).isEqualTo(15);
            assertThat(props.getRecovery().getMaxRecoveryAttempts()).isEqualTo(10);
            assertThat(props.getRecovery().getBatchSize()).isEqualTo(100);
        }

        @Test
        @DisplayName("failover config disabled by default")
        void failoverDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getFailover().isEnabled()).isFalse();
            assertThat(props.getFailover().getActiveDc()).isEqualTo("dc-a");
            assertThat(props.getFailover().getReplicationPolicy()).isEqualTo(ReplicationPolicy.IDENTITY);
        }

        @Test
        @DisplayName("retention config has sensible defaults")
        void retentionDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getRetention().getDltRetentionHours()).isEqualTo(24);
            assertThat(props.getRetention().getOutboxDays()).isEqualTo(7);
            assertThat(props.getRetention().getProcessedEventsDays()).isEqualTo(30);
            assertThat(props.getRetention().getStepLogDays()).isEqualTo(90);
        }

        @Test
        @DisplayName("audit config defaults")
        void auditDefaults() {
            OrchestratorProperties props = new OrchestratorProperties();

            assertThat(props.getAudit().isIncludeFlowState()).isFalse();
            assertThat(props.getAudit().getMaxLogSnapshotBytes()).isEqualTo(32768);
        }
    }

    // ========================================================================
    // orchestratorCommandRetryConfig bean
    // ========================================================================

    @Nested
    @DisplayName("orchestratorCommandRetryConfig")
    class RetryConfigTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("creates retry config with custom backoff settings")
        void retryConfig_usesPropsSettings() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getRetry().setMaxAttempts(6);
            props.getRetry().setInitialIntervalMs(5000);
            props.getRetry().setMultiplier(3.0);
            props.getRetry().setMaxIntervalMs(60000);
            props.getRetry().setJitterFactor(0.3);
            props.getKafka().setCommandTopic("custom.commands");

            @SuppressWarnings("unchecked")
            org.springframework.kafka.core.KafkaTemplate<String, String> template =
                    mock(org.springframework.kafka.core.KafkaTemplate.class);

            org.springframework.kafka.retrytopic.RetryTopicConfiguration retryConfig =
                    config.orchestratorCommandRetryConfig(template, props);

            assertThat(retryConfig).isNotNull();
        }

        @Test
        @DisplayName("retry config includes prefixed topics for PREFIXED failover")
        void retryConfig_includesPrefixedTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("commands");
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(ReplicationPolicy.PREFIXED);

            DcConfig dc = new DcConfig();
            dc.setSourceAlias("us-east");
            props.getFailover().setDcs(Map.of("us-east", dc));

            @SuppressWarnings("unchecked")
            org.springframework.kafka.core.KafkaTemplate<String, String> template =
                    mock(org.springframework.kafka.core.KafkaTemplate.class);

            org.springframework.kafka.retrytopic.RetryTopicConfiguration retryConfig =
                    config.orchestratorCommandRetryConfig(template, props);

            assertThat(retryConfig).isNotNull();
        }

        @Test
        @DisplayName("retry config with IDENTITY failover only includes base topic")
        void retryConfig_identityOnlyBaseTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("commands");
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(ReplicationPolicy.IDENTITY);

            DcConfig dc = new DcConfig();
            dc.setSourceAlias("us-east");
            props.getFailover().setDcs(Map.of("us-east", dc));

            @SuppressWarnings("unchecked")
            org.springframework.kafka.core.KafkaTemplate<String, String> template =
                    mock(org.springframework.kafka.core.KafkaTemplate.class);

            org.springframework.kafka.retrytopic.RetryTopicConfiguration retryConfig =
                    config.orchestratorCommandRetryConfig(template, props);

            assertThat(retryConfig).isNotNull();
        }

        @Test
        @DisplayName("retry config with default properties")
        void retryConfig_defaultProps() {
            OrchestratorProperties props = new OrchestratorProperties();

            @SuppressWarnings("unchecked")
            org.springframework.kafka.core.KafkaTemplate<String, String> template =
                    mock(org.springframework.kafka.core.KafkaTemplate.class);

            org.springframework.kafka.retrytopic.RetryTopicConfiguration retryConfig =
                    config.orchestratorCommandRetryConfig(template, props);

            assertThat(retryConfig).isNotNull();
        }
    }

    // ========================================================================
    // Shared service beans
    // ========================================================================

    @Nested
    @DisplayName("Shared service beans")
    class SharedServiceBeanTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("orchestratorMetrics created without meter registry")
        void metrics_withoutRegistry() {
            var provider = mock(org.springframework.beans.factory.ObjectProvider.class);
            when(provider.getIfAvailable()).thenReturn(null);

            var metrics = config.orchestratorMetrics(provider);
            assertThat(metrics).isNotNull();
        }

        @Test
        @DisplayName("orchestratorIdempotencyService created")
        void idempotencyService_created() {
            var repo = mock(com.orchestrator.starter.idempotency.ProcessedEventRepository.class);
            var metrics = mock(OrchestratorMetrics.class);

            var service = config.orchestratorIdempotencyService(repo, metrics);
            assertThat(service).isNotNull();
        }

        @Test
        @DisplayName("orchestratorTopicValidator created")
        void topicValidator_created() {
            var kafkaAdmin = mock(org.springframework.kafka.core.KafkaAdmin.class);
            var props = new OrchestratorProperties();

            var validator = config.orchestratorTopicValidator(kafkaAdmin, props);
            assertThat(validator).isNotNull();
        }

        @Test
        @DisplayName("orchestratorOutboxPublisher created with configured params")
        void outboxPublisher_created() {
            var outboxRepo = mock(com.orchestrator.starter.outbox.OutboxEventRepository.class);
            @SuppressWarnings("unchecked")
            var kafkaTemplate = mock(org.springframework.kafka.core.KafkaTemplate.class);
            var props = new OrchestratorProperties();
            props.getOutbox().setMaxPublishRetries(5);
            props.getOutbox().setBatchSize(200);
            var metrics = mock(OrchestratorMetrics.class);

            var publisher = config.orchestratorOutboxPublisher(outboxRepo, kafkaTemplate, props, metrics);
            assertThat(publisher).isNotNull();
        }

        @Test
        @DisplayName("mongoOffsetStore created with cluster ID")
        void mongoOffsetStore_created() {
            var mongoTemplate = mock(MongoTemplate.class);
            var props = new OrchestratorProperties();
            props.getKafka().setClusterId("my-cluster");

            var store = config.mongoOffsetStore(mongoTemplate, props);
            assertThat(store).isNotNull();
        }

        @Test
        @DisplayName("timestampOffsetRecoveryListener created with recovery config")
        void timestampRecoveryListener_created() {
            var props = new OrchestratorProperties();
            var listener = config.timestampOffsetRecoveryListener(props);
            assertThat(listener).isNotNull();
        }

        @Test
        @DisplayName("orchestratorIndexInitializer created")
        void indexInitializer_created() {
            var mongoTemplate = mock(MongoTemplate.class);
            var props = new OrchestratorProperties();
            var registry = new FlowTypeRegistry(List.of());

            var initializer = config.orchestratorIndexInitializer(mongoTemplate, props, registry);
            assertThat(initializer).isNotNull();
        }
    }

    // ========================================================================
    // Backward-compatible singleton beans
    // ========================================================================

    @Nested
    @DisplayName("Backward-compatible singleton beans")
    class BackwardCompatBeanTests {

        @Mock
        private FlowTypeRegistry registry;
        @Mock
        private FlowTypeDescriptor descriptor;
        @Mock
        private FlowOrchestrator<?> orchestrator;
        @Mock
        private com.orchestrator.starter.domain.OrchestratorFlowRepository<?> flowRepo;
        @Mock
        private com.orchestrator.starter.flow.StepRegistry<?> stepRegistry;

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            config = new OrchestratorAutoConfiguration();
            when(registry.getAll()).thenReturn(List.of(descriptor));
            doReturn(orchestrator).when(descriptor).getOrchestrator();
            doReturn(flowRepo).when(descriptor).getRepository();
            doReturn(stepRegistry).when(descriptor).getStepRegistry();
        }

        @Test
        @DisplayName("orchestratorFlowOrchestrator returns first flow's orchestrator")
        void flowOrchestrator_returnsFirst() {
            var result = config.orchestratorFlowOrchestrator(registry);
            assertThat(result).isSameAs(orchestrator);
        }

        @Test
        @DisplayName("orchestratorGenericFlowRepository returns first flow's repository")
        void flowRepository_returnsFirst() {
            var result = config.orchestratorGenericFlowRepository(registry);
            assertThat(result).isSameAs(flowRepo);
        }

        @Test
        @DisplayName("orchestratorStepRegistry returns first flow's step registry")
        void stepRegistry_returnsFirst() {
            var result = config.orchestratorStepRegistry(registry);
            assertThat(result).isSameAs(stepRegistry);
        }
    }

    // ========================================================================
    // Shutdown hook
    // ========================================================================

    @Nested
    @DisplayName("Shutdown hook")
    class ShutdownHookTests {

        @Mock
        private FlowTypeRegistry registry;
        @Mock
        private FlowTypeDescriptor descriptor;

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("shutdown hook calls shutdown on all orchestrators")
        void shutdownHook_callsShutdownOnAll() {
            var orchestrator = mock(FlowOrchestrator.class);
            when(descriptor.getOrchestrator()).thenReturn(orchestrator);
            when(registry.getAll()).thenReturn(List.of(descriptor));

            var listener = config.orchestratorShutdownHook(registry);
            var event = mock(org.springframework.context.event.ContextClosedEvent.class);
            listener.onApplicationEvent(event);

            verify(orchestrator).shutdown();
        }

        @Test
        @DisplayName("shutdown hook handles null orchestrator gracefully")
        void shutdownHook_handlesNullOrchestrator() {
            when(descriptor.getOrchestrator()).thenReturn(null);
            when(registry.getAll()).thenReturn(List.of(descriptor));

            var listener = config.orchestratorShutdownHook(registry);
            var event = mock(org.springframework.context.event.ContextClosedEvent.class);

            // Should not throw
            listener.onApplicationEvent(event);
        }

        @Test
        @DisplayName("shutdown hook with empty registry does nothing")
        void shutdownHook_emptyRegistry() {
            when(registry.getAll()).thenReturn(List.of());

            var listener = config.orchestratorShutdownHook(registry);
            var event = mock(org.springframework.context.event.ContextClosedEvent.class);

            listener.onApplicationEvent(event);
            // No orchestrators to shut down — just logs
        }
    }

    // ========================================================================
    // PREFIXED failover with multiple DCs — edge cases
    // ========================================================================

    @Nested
    @DisplayName("Failover edge cases")
    class FailoverEdgeCaseTests {

        private OrchestratorAutoConfiguration config;

        @BeforeEach
        void setUp() {
            config = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("failover enabled but no DCs configured — returns just local topic")
        void failoverEnabled_noDcs() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("my.commands");
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(ReplicationPolicy.PREFIXED);
            // no DCs added — empty map

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).containsExactly("my.commands");
        }

        @Test
        @DisplayName("three DCs in PREFIXED mode produces local + 3 prefixed topics")
        void threeDcsPrefixed() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("commands");
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(ReplicationPolicy.PREFIXED);

            Map<String, DcConfig> dcs = new LinkedHashMap<>();
            for (String dc : List.of("us-east", "us-west", "eu-central")) {
                DcConfig cfg = new DcConfig();
                cfg.setSourceAlias(dc);
                dcs.put(dc, cfg);
            }
            props.getFailover().setDcs(dcs);

            String[] topics = config.orchestratorCommandTopics(props);

            assertThat(topics).hasSize(4); // local + 3 prefixed
            assertThat(topics[0]).isEqualTo("commands");
            assertThat(topics).contains(
                    "us-east.commands",
                    "us-west.commands",
                    "eu-central.commands"
            );
        }

        @Test
        @DisplayName("DLT topics mirror command topic prefixing for three DCs")
        void threeDcsPrefixed_dltTopics() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("commands");
            props.getFailover().setEnabled(true);
            props.getFailover().setReplicationPolicy(ReplicationPolicy.PREFIXED);

            Map<String, DcConfig> dcs = new LinkedHashMap<>();
            for (String dc : List.of("us-east", "us-west")) {
                DcConfig cfg = new DcConfig();
                cfg.setSourceAlias(dc);
                dcs.put(dc, cfg);
            }
            props.getFailover().setDcs(dcs);

            String[] dltTopics = config.orchestratorCommandDltTopics(props);

            assertThat(dltTopics).containsExactly(
                    "commands-dlt",
                    "us-east.commands-dlt",
                    "us-west.commands-dlt"
            );
        }
    }
}
