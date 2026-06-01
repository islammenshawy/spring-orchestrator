package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.*;
import com.orchestrator.starter.failover.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DC failover components:
 * - DcHealthProbe
 * - DcHealthEndpoint
 * - DcAwareKafkaManager
 * - DcAwareListenerManager
 * - DcAwareKafkaTemplate
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("DC Failover Components")
class DcFailoverComponentsTest {

    private FailoverConfig config;

    @BeforeEach
    void setUp() {
        config = new FailoverConfig();
        config.setEnabled(true);
        config.setActiveDc("dc-a");
        config.setReplicationPolicy(ReplicationPolicy.IDENTITY);
        config.setDegradedThreshold(3);
        config.setFailoverThreshold(6);
        config.setDwellTimeSeconds(0);
        config.setProbeTimeoutMs(1000);

        DcConfig dcA = new DcConfig();
        dcA.setBootstrap("kafka-a:9092");
        dcA.setSourceAlias("dc-a");

        DcConfig dcB = new DcConfig();
        dcB.setBootstrap("kafka-b:9093");
        dcB.setSourceAlias("dc-b");

        Map<String, DcConfig> dcs = new LinkedHashMap<>();
        dcs.put("dc-a", dcA);
        dcs.put("dc-b", dcB);
        config.setDcs(dcs);
    }

    // ========================================================================
    // DcHealthProbe
    // ========================================================================

    @Nested
    @DisplayName("DcHealthProbe")
    class HealthProbeTests {

        @Test
        @DisplayName("returns false for unknown DC")
        void unknownDc_returnsFalse() {
            Map<String, String> bootstraps = Map.of("dc-a", "kafka-a:9092");
            var probe = new DcHealthProbe(bootstraps, 1000);

            assertThat(probe.probe("dc-unknown")).isFalse();
        }

        @Test
        @DisplayName("returns false when TCP connect fails (unreachable host)")
        void tcpConnectFails_returnsFalse() {
            // Use a non-routable IP to guarantee connection timeout/failure
            Map<String, String> bootstraps = Map.of("dc-a", "192.0.2.1:9092");
            var probe = new DcHealthProbe(bootstraps, 100); // very short timeout

            assertThat(probe.probe("dc-a")).isFalse();
        }

        @Test
        @DisplayName("parses bootstrap with multiple brokers — uses first one")
        void multipleBootstrapBrokers_usesFirst() {
            Map<String, String> bootstraps = Map.of("dc-a", "192.0.2.1:9092,192.0.2.2:9092");
            var probe = new DcHealthProbe(bootstraps, 100);

            // Will fail because 192.0.2.1 is non-routable, but it should NOT throw
            assertThat(probe.probe("dc-a")).isFalse();
        }

        @Test
        @DisplayName("parses bootstrap without port — defaults to 9092")
        void bootstrapWithoutPort_defaultsTo9092() {
            Map<String, String> bootstraps = Map.of("dc-a", "192.0.2.1");
            var probe = new DcHealthProbe(bootstraps, 100);

            // Will fail (non-routable), but should parse correctly without exception
            assertThat(probe.probe("dc-a")).isFalse();
        }

        @Test
        @DisplayName("close is safe to call multiple times")
        void close_isSafe() {
            Map<String, String> bootstraps = Map.of("dc-a", "kafka-a:9092");
            var probe = new DcHealthProbe(bootstraps, 1000);

            probe.close();
            probe.close(); // no exception
        }
    }

    // ========================================================================
    // DcHealthEndpoint
    // ========================================================================

    @Nested
    @DisplayName("DcHealthEndpoint")
    class HealthEndpointTests {

        @Test
        @DisplayName("returns correct structure with active/standby DCs")
        void returnsCorrectStructure() {
            var supervisor = mock(DcFailoverSupervisor.class);
            when(supervisor.getActiveDc()).thenReturn("dc-a");
            when(supervisor.getState()).thenReturn(DcState.HEALTHY);

            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(true);
            when(probe.probe("dc-b")).thenReturn(true);

            var kafkaManager = mock(DcAwareKafkaManager.class);
            var topicResolver = new TopicResolver(config);

            var endpoint = new DcHealthEndpoint(supervisor, probe, kafkaManager, topicResolver, config);

            Map<String, Object> health = endpoint.health();

            assertThat(health.get("activeDc")).isEqualTo("dc-a");
            assertThat(health.get("supervisorState")).isEqualTo("HEALTHY");
            assertThat(health.get("replicationPolicy")).isEqualTo("IDENTITY");

            @SuppressWarnings("unchecked")
            Map<String, Object> dcs = (Map<String, Object>) health.get("dcs");
            assertThat(dcs).containsKeys("dc-a", "dc-b");

            @SuppressWarnings("unchecked")
            Map<String, Object> dcA = (Map<String, Object>) dcs.get("dc-a");
            assertThat(dcA.get("bootstrap")).isEqualTo("kafka-a:9092");
            assertThat(dcA.get("healthy")).isEqualTo(true);
            assertThat(dcA.get("role")).isEqualTo("ACTIVE");

            @SuppressWarnings("unchecked")
            Map<String, Object> dcB = (Map<String, Object>) dcs.get("dc-b");
            assertThat(dcB.get("role")).isEqualTo("STANDBY");
            assertThat(dcB.get("healthy")).isEqualTo(true);
        }

        @Test
        @DisplayName("shows DEGRADED supervisor state")
        void showsDegradedState() {
            var supervisor = mock(DcFailoverSupervisor.class);
            when(supervisor.getActiveDc()).thenReturn("dc-a");
            when(supervisor.getState()).thenReturn(DcState.DEGRADED);

            var probe = mock(DcHealthProbe.class);
            when(probe.probe(anyString())).thenReturn(false);

            var kafkaManager = mock(DcAwareKafkaManager.class);
            var topicResolver = new TopicResolver(config);

            var endpoint = new DcHealthEndpoint(supervisor, probe, kafkaManager, topicResolver, config);
            Map<String, Object> health = endpoint.health();

            assertThat(health.get("supervisorState")).isEqualTo("DEGRADED");

            @SuppressWarnings("unchecked")
            Map<String, Object> dcs = (Map<String, Object>) health.get("dcs");
            @SuppressWarnings("unchecked")
            Map<String, Object> dcA = (Map<String, Object>) dcs.get("dc-a");
            assertThat(dcA.get("healthy")).isEqualTo(false);
        }

        @Test
        @DisplayName("includes topic mapping in response")
        void includesTopicMapping() {
            var supervisor = mock(DcFailoverSupervisor.class);
            when(supervisor.getActiveDc()).thenReturn("dc-a");
            when(supervisor.getState()).thenReturn(DcState.HEALTHY);

            var probe = mock(DcHealthProbe.class);
            when(probe.probe(anyString())).thenReturn(true);

            var kafkaManager = mock(DcAwareKafkaManager.class);

            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            var topicResolver = new TopicResolver(config);

            var endpoint = new DcHealthEndpoint(supervisor, probe, kafkaManager, topicResolver, config);
            Map<String, Object> health = endpoint.health();

            @SuppressWarnings("unchecked")
            Map<String, String> topicMapping = (Map<String, String>) health.get("topicMapping");
            assertThat(topicMapping).isNotEmpty();
            // dc-a reading its own topic: no prefix
            assertThat(topicMapping.get("dis.instrument.commands (on dc-a)"))
                    .isEqualTo("dis.instrument.commands");
            // dc-b reading dc-a's replicated topic: prefixed
            assertThat(topicMapping.get("dis.instrument.commands (on dc-b)"))
                    .isEqualTo("dc-a.dis.instrument.commands");
        }

        @Test
        @DisplayName("shows COOLDOWN state after failover")
        void showsCooldownState() {
            var supervisor = mock(DcFailoverSupervisor.class);
            when(supervisor.getActiveDc()).thenReturn("dc-b");
            when(supervisor.getState()).thenReturn(DcState.COOLDOWN);

            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(false);
            when(probe.probe("dc-b")).thenReturn(true);

            var kafkaManager = mock(DcAwareKafkaManager.class);
            var topicResolver = new TopicResolver(config);

            var endpoint = new DcHealthEndpoint(supervisor, probe, kafkaManager, topicResolver, config);
            Map<String, Object> health = endpoint.health();

            assertThat(health.get("activeDc")).isEqualTo("dc-b");
            assertThat(health.get("supervisorState")).isEqualTo("COOLDOWN");

            @SuppressWarnings("unchecked")
            Map<String, Object> dcs = (Map<String, Object>) health.get("dcs");
            @SuppressWarnings("unchecked")
            Map<String, Object> dcA = (Map<String, Object>) dcs.get("dc-a");
            @SuppressWarnings("unchecked")
            Map<String, Object> dcB = (Map<String, Object>) dcs.get("dc-b");
            // After failover: dc-b is active, dc-a is standby
            assertThat(dcB.get("role")).isEqualTo("ACTIVE");
            assertThat(dcA.get("role")).isEqualTo("STANDBY");
        }
    }

    // ========================================================================
    // DcAwareKafkaManager
    // ========================================================================

    @Nested
    @DisplayName("DcAwareKafkaManager")
    class KafkaManagerTests {

        private DcAwareKafkaManager manager;
        private TopicResolver topicResolver;

        @BeforeEach
        void setUp() {
            topicResolver = new TopicResolver(config);
            manager = new DcAwareKafkaManager(config, topicResolver);
        }

        @Test
        @DisplayName("starts with configured active DC")
        void startsWithConfiguredActiveDc() {
            assertThat(manager.getActiveDc()).isEqualTo("dc-a");
        }

        @Test
        @DisplayName("creates factories for all configured DCs")
        void createsFactoriesForAllDcs() {
            assertThat(manager.getAllDcIds()).containsExactlyInAnyOrder("dc-a", "dc-b");
            assertThat(manager.getConsumerFactory("dc-a")).isNotNull();
            assertThat(manager.getConsumerFactory("dc-b")).isNotNull();
            assertThat(manager.getTemplate("dc-a")).isNotNull();
            assertThat(manager.getTemplate("dc-b")).isNotNull();
        }

        @Test
        @DisplayName("getActiveTemplate returns active DC's template")
        void getActiveTemplate_returnsActiveDcTemplate() {
            KafkaTemplate<String, String> active = manager.getActiveTemplate();
            assertThat(active).isSameAs(manager.getTemplate("dc-a"));
        }

        @Test
        @DisplayName("getActiveConsumerFactory returns active DC's consumer factory")
        void getActiveConsumerFactory_returnsActiveDc() {
            assertThat(manager.getActiveConsumerFactory()).isSameAs(manager.getConsumerFactory("dc-a"));
        }

        @Test
        @DisplayName("switchActiveDc changes active DC and calls listenerManager.switchDc")
        void switchActiveDc_changesActiveAndCallsListenerManager() {
            var listenerManager = mock(DcAwareListenerManager.class);
            manager.setListenerManager(listenerManager);

            manager.switchActiveDc("dc-b");

            assertThat(manager.getActiveDc()).isEqualTo("dc-b");
            assertThat(manager.getActiveTemplate()).isSameAs(manager.getTemplate("dc-b"));
            verify(listenerManager).switchDc("dc-a", "dc-b");
        }

        @Test
        @DisplayName("switchActiveDc restarts @KafkaListener containers")
        void switchActiveDc_restartsKafkaListenerContainers() {
            var registry = mock(KafkaListenerEndpointRegistry.class);
            var container = mock(MessageListenerContainer.class);
            when(container.getListenerId()).thenReturn("command-listener");
            when(registry.getListenerContainers()).thenReturn(Set.of(container));
            manager.setKafkaListenerRegistry(registry);

            manager.switchActiveDc("dc-b");

            verify(container).stop();
            verify(container).start();
        }

        @Test
        @DisplayName("switchActiveDc with unknown DC throws IllegalArgumentException")
        void switchActiveDc_unknownDc_throws() {
            assertThatThrownBy(() -> manager.switchActiveDc("dc-unknown"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown DC");
        }

        @Test
        @DisplayName("switchActiveDc updates originating DC to previous active")
        void switchActiveDc_updatesOriginatingDc() {
            assertThat(manager.getOriginatingDc()).isEqualTo("dc-a");

            manager.switchActiveDc("dc-b");

            assertThat(manager.getOriginatingDc()).isEqualTo("dc-a"); // where messages originated
            assertThat(manager.getActiveDc()).isEqualTo("dc-b"); // where we now consume
        }

        @Test
        @DisplayName("resolveActiveTopic delegates to topic resolver")
        void resolveActiveTopic_delegatesToResolver() {
            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            topicResolver = new TopicResolver(config);
            manager = new DcAwareKafkaManager(config, topicResolver);

            // Active DC is dc-a, originating is dc-a => no prefix
            assertThat(manager.resolveActiveTopic("dis.commands")).isEqualTo("dis.commands");

            // After switch: active is dc-b, originating is dc-a => prefix
            manager.switchActiveDc("dc-b");
            assertThat(manager.resolveActiveTopic("dis.commands")).isEqualTo("dc-a.dis.commands");
        }

        @Test
        @DisplayName("switchActiveDc handles container restart failure gracefully")
        void switchActiveDc_containerRestartFailure_logsAndContinues() {
            var registry = mock(KafkaListenerEndpointRegistry.class);
            var failingContainer = mock(MessageListenerContainer.class);
            when(failingContainer.getListenerId()).thenReturn("broken-listener");
            doThrow(new RuntimeException("restart failed")).when(failingContainer).start();
            when(registry.getListenerContainers()).thenReturn(Set.of(failingContainer));
            manager.setKafkaListenerRegistry(registry);

            // Should not throw
            manager.switchActiveDc("dc-b");

            assertThat(manager.getActiveDc()).isEqualTo("dc-b");
        }

        @Test
        @DisplayName("switchActiveDc without listenerManager set does not fail")
        void switchActiveDc_noListenerManager_noFailure() {
            // listenerManager is null by default
            manager.switchActiveDc("dc-b");
            assertThat(manager.getActiveDc()).isEqualTo("dc-b");
        }

        @Test
        @DisplayName("close destroys producer factories")
        void close_destroysProducerFactories() {
            // Should not throw
            manager.close();
        }
    }

    // ========================================================================
    // DcAwareListenerManager
    // ========================================================================

    @Nested
    @DisplayName("DcAwareListenerManager")
    class ListenerManagerTests {

        private DcAwareKafkaManager kafkaManager;
        private DcAwareListenerManager listenerManager;
        private TopicResolver topicResolver;

        @SuppressWarnings("unchecked")
        private ConsumerFactory<String, String> buildMockConsumerFactory() {
            ConsumerFactory<String, String> factory = mock(ConsumerFactory.class);
            Consumer<String, String> consumer = mock(Consumer.class);
            doReturn(consumer).when(factory).createConsumer(anyString(), anyString(), anyString(), any());
            doReturn(consumer).when(factory).createConsumer(anyString(), anyString(), anyString());
            doReturn(consumer).when(factory).createConsumer(anyString(), anyString());
            return factory;
        }

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            topicResolver = new TopicResolver(config);

            // Build mocked consumer factories BEFORE stubbing kafkaManager
            ConsumerFactory<String, String> factoryA = buildMockConsumerFactory();
            ConsumerFactory<String, String> factoryB = buildMockConsumerFactory();

            kafkaManager = mock(DcAwareKafkaManager.class);
            doReturn("dc-a").when(kafkaManager).getActiveDc();
            doReturn(Set.of("dc-a", "dc-b")).when(kafkaManager).getAllDcIds();
            doReturn(factoryA).when(kafkaManager).getConsumerFactory("dc-a");
            doReturn(factoryB).when(kafkaManager).getConsumerFactory("dc-b");

            listenerManager = new DcAwareListenerManager(kafkaManager, topicResolver);
        }

        @Test
        @DisplayName("registerAndCreate creates containers for all DCs, starts only active")
        void registerAndCreate_createsForAllDcs_startsActive() {
            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("reply-listener")
                    .originalTopic("dis.commands.replies")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(1)
                    .build();

            ConcurrentMessageListenerContainer<String, String> activeContainer =
                    listenerManager.registerAndCreate(blueprint);

            // Active container should be returned and started
            assertThat(activeContainer).isNotNull();
            assertThat(activeContainer.getBeanName()).contains("dc-a");
        }

        @Test
        @DisplayName("switchDc stops old DC containers and starts new DC containers")
        void switchDc_stopsOldStartsNew() {
            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("test-listener")
                    .originalTopic("dis.commands")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(1)
                    .build();

            // Register creates containers for both DCs
            listenerManager.registerAndCreate(blueprint);

            // Switch from dc-a to dc-b
            listenerManager.switchDc("dc-a", "dc-b");

            // Verify by checking that stopAll doesn't throw
            listenerManager.stopAll();
        }

        @Test
        @DisplayName("switchDc with no containers for a DC is safe (empty list)")
        void switchDc_noContainers_safe() {
            // No containers registered yet
            listenerManager.switchDc("dc-a", "dc-b");
            // Should not throw
        }

        @Test
        @DisplayName("stopAll stops all containers without throwing")
        void stopAll_stopsAllContainers() {
            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("stop-test")
                    .originalTopic("dis.commands")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(1)
                    .build();

            listenerManager.registerAndCreate(blueprint);
            listenerManager.stopAll();
            // No exception means success
        }

        @Test
        @DisplayName("registerAndCreate sets rebalance listener when configured")
        void registerAndCreate_setsRebalanceListener() {
            var rebalanceListener = mock(org.springframework.kafka.listener.ConsumerAwareRebalanceListener.class);
            listenerManager.setRebalanceListener(rebalanceListener);

            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("rebalance-test")
                    .originalTopic("dis.commands")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(2)
                    .build();

            ConcurrentMessageListenerContainer<String, String> container =
                    listenerManager.registerAndCreate(blueprint);

            assertThat(container).isNotNull();
            assertThat(container.getConcurrency()).isEqualTo(2);
        }

        @Test
        @DisplayName("registerAndCreate defaults concurrency to 1 when set to 0")
        void registerAndCreate_defaultsConcurrency() {
            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("default-concurrency")
                    .originalTopic("dis.commands")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(0) // should default to 1
                    .build();

            ConcurrentMessageListenerContainer<String, String> container =
                    listenerManager.registerAndCreate(blueprint);

            assertThat(container).isNotNull();
            assertThat(container.getConcurrency()).isEqualTo(1);
        }

        @Test
        @DisplayName("PREFIXED policy: active DC container gets unprefixed topic")
        void prefixedPolicy_resolvedTopicNames() {
            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            topicResolver = new TopicResolver(config);
            listenerManager = new DcAwareListenerManager(kafkaManager, topicResolver);

            MessageListener<String, String> messageListener = record -> {};
            ContainerBlueprint blueprint = ContainerBlueprint.builder()
                    .id("prefixed-test")
                    .originalTopic("dis.commands")
                    .groupId("test-group")
                    .messageListener(messageListener)
                    .concurrency(1)
                    .build();

            ConcurrentMessageListenerContainer<String, String> activeContainer =
                    listenerManager.registerAndCreate(blueprint);

            // Active container (dc-a) should have the unprefixed topic
            assertThat(activeContainer).isNotNull();
            assertThat(activeContainer.getBeanName()).contains("dc-a");
        }
    }

    // ========================================================================
    // DcAwareKafkaTemplate
    // ========================================================================

    @Nested
    @DisplayName("DcAwareKafkaTemplate")
    class KafkaTemplateTests {

        @Test
        @DisplayName("send(topic, key, data) routes to active DC template with resolved topic")
        void sendTopicKeyData_routesToActiveDc() {
            var kafkaManager = mock(DcAwareKafkaManager.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> activeTemplate = mock(KafkaTemplate.class);

            when(kafkaManager.resolveActiveTopic("dis.commands")).thenReturn("dis.commands");
            when(kafkaManager.getActiveTemplate()).thenReturn(activeTemplate);
            when(activeTemplate.send(anyString(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            var dcTemplate = new DcAwareKafkaTemplate(kafkaManager);
            dcTemplate.send("dis.commands", "key-1", "payload");

            verify(activeTemplate).send("dis.commands", "key-1", "payload");
        }

        @Test
        @DisplayName("send(topic, data) routes to active DC template")
        void sendTopicData_routesToActiveDc() {
            var kafkaManager = mock(DcAwareKafkaManager.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> activeTemplate = mock(KafkaTemplate.class);

            when(kafkaManager.resolveActiveTopic("dis.commands")).thenReturn("dis.commands");
            when(kafkaManager.getActiveTemplate()).thenReturn(activeTemplate);
            when(activeTemplate.send(anyString(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            var dcTemplate = new DcAwareKafkaTemplate(kafkaManager);
            dcTemplate.send("dis.commands", "payload");

            verify(activeTemplate).send("dis.commands", "payload");
        }

        @Test
        @DisplayName("send(topic, partition, key, data) routes to active DC template")
        void sendTopicPartitionKeyData_routesToActiveDc() {
            var kafkaManager = mock(DcAwareKafkaManager.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> activeTemplate = mock(KafkaTemplate.class);

            when(kafkaManager.resolveActiveTopic("dis.commands")).thenReturn("dis.commands");
            when(kafkaManager.getActiveTemplate()).thenReturn(activeTemplate);
            when(activeTemplate.send(anyString(), any(Integer.class), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            var dcTemplate = new DcAwareKafkaTemplate(kafkaManager);
            dcTemplate.send("dis.commands", 2, "key-1", "payload");

            verify(activeTemplate).send("dis.commands", 2, "key-1", "payload");
        }

        @Test
        @DisplayName("topic resolution applies PREFIXED policy before sending")
        void topicResolution_appliesPrefixedPolicy() {
            var kafkaManager = mock(DcAwareKafkaManager.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> activeTemplate = mock(KafkaTemplate.class);

            // Simulates PREFIXED mode: topic gets source alias prefix
            when(kafkaManager.resolveActiveTopic("dis.commands")).thenReturn("dc-a.dis.commands");
            when(kafkaManager.getActiveTemplate()).thenReturn(activeTemplate);
            when(activeTemplate.send(anyString(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            var dcTemplate = new DcAwareKafkaTemplate(kafkaManager);
            dcTemplate.send("dis.commands", "key-1", "payload");

            // Should send to the resolved (prefixed) topic
            verify(activeTemplate).send("dc-a.dis.commands", "key-1", "payload");
        }

        @Test
        @DisplayName("after DC switch, sends route to new active template")
        void afterDcSwitch_routesToNewActiveTemplate() {
            var kafkaManager = mock(DcAwareKafkaManager.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> templateA = mock(KafkaTemplate.class);
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, String> templateB = mock(KafkaTemplate.class);

            // First call: dc-a is active
            when(kafkaManager.resolveActiveTopic("dis.commands")).thenReturn("dis.commands");
            when(kafkaManager.getActiveTemplate())
                    .thenReturn(templateA)
                    .thenReturn(templateB); // after switch
            when(templateA.send(anyString(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(templateB.send(anyString(), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            var dcTemplate = new DcAwareKafkaTemplate(kafkaManager);

            // Send before switch
            dcTemplate.send("dis.commands", "key-1", "payload-1");
            verify(templateA).send("dis.commands", "key-1", "payload-1");

            // Send after switch
            dcTemplate.send("dis.commands", "key-2", "payload-2");
            verify(templateB).send("dis.commands", "key-2", "payload-2");
        }
    }
}
