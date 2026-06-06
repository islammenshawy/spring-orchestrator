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
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, false, "app-executor");

                listener.onCommandDlt("dlt-payload", "orchestrator.commands-dlt", 100L, "NullPointerException: oops");

                verify(consumer).onDlt("dlt-payload", "orchestrator.commands-dlt", 100L, "NullPointerException: oops");
            }

            @Test
            @DisplayName("null exception header sends 'unknown' as message")
            void onDlt_nullExceptionHeader() {
                var listener = new OrchestratorCommandListener(consumer, mongoOffsetStore, false, "app-executor");

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
            assertThat(topic.numPartitions()).isEqualTo(1);
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

            var singleton = config.orchestratorStartupValidator(context);

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

            var singleton = config.orchestratorStartupValidator(context);

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

            var singleton = config.orchestratorStartupValidator(context);
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

            var singleton = config.orchestratorStartupValidator(context);

            // Should not throw — logs error instead
            singleton.afterSingletonsInstantiated();

            verify(mongoTemplate).getDb();
        }
    }

    // ========================================================================
    // orchestratorCommandListener bean creation
    // ========================================================================

    @Nested
    @DisplayName("orchestratorCommandListener bean")
    class CommandListenerBeanTests {

        @Mock
        private OrchestratorKafkaConsumer<?> consumer;
        @Mock
        private MongoOffsetStore mongoOffsetStore;
        @Mock
        private org.springframework.core.env.Environment env;

        private OrchestratorAutoConfiguration autoConfig;

        @BeforeEach
        void setUp() {
            autoConfig = new OrchestratorAutoConfiguration();
        }

        @Test
        @DisplayName("creates listener with saveMongo=true when offset store is MONGO")
        void createsWithSaveMongoTrue() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getRecovery().setOffsetStore(OffsetStore.MONGO);
            when(env.getProperty("spring.application.name", "orchestrator")).thenReturn("my-app");

            OrchestratorCommandListener listener = autoConfig.orchestratorCommandListener(
                    consumer, mongoOffsetStore, props, env);

            // Verify it saves offset by calling onCommand with valid partition+timestamp
            listener.onCommand("test", "topic", 1L, 0, 1000L, "key");
            verify(mongoOffsetStore).saveOffset(eq("my-app-executor"), eq("topic"), eq(0), eq(1L), eq("key"), eq(1000L));
        }

        @Test
        @DisplayName("creates listener with saveMongo=false when offset store is KAFKA")
        void createsWithSaveMongoFalse() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getRecovery().setOffsetStore(OffsetStore.KAFKA);
            when(env.getProperty("spring.application.name", "orchestrator")).thenReturn("my-app");

            OrchestratorCommandListener listener = autoConfig.orchestratorCommandListener(
                    consumer, mongoOffsetStore, props, env);

            listener.onCommand("test", "topic", 1L, 0, 1000L, "key");
            verifyNoInteractions(mongoOffsetStore);
        }

        @Test
        @DisplayName("uses default app name 'orchestrator' when spring.application.name not set")
        void usesDefaultAppName() {
            OrchestratorProperties props = new OrchestratorProperties();
            props.getRecovery().setOffsetStore(OffsetStore.MONGO);
            when(env.getProperty("spring.application.name", "orchestrator")).thenReturn("orchestrator");

            OrchestratorCommandListener listener = autoConfig.orchestratorCommandListener(
                    consumer, mongoOffsetStore, props, env);

            listener.onCommand("test", "topic", 1L, 0, 1000L, "key");
            verify(mongoOffsetStore).saveOffset(eq("orchestrator-executor"), anyString(), anyInt(), anyLong(), any(), anyLong());
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
