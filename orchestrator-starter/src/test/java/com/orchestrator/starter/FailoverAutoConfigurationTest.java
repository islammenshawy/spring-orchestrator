package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.*;
import com.orchestrator.starter.failover.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for FailoverAutoConfiguration bean creation,
 * DcAwareConsumerFactory delegation, and MongoTransactionAutoConfiguration.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("Failover & Mongo Transaction AutoConfiguration")
class FailoverAutoConfigurationTest {

    private OrchestratorProperties props;
    private FailoverConfig failoverConfig;

    @BeforeEach
    void setUp() {
        props = new OrchestratorProperties();
        failoverConfig = props.getFailover();
        failoverConfig.setEnabled(true);
        failoverConfig.setActiveDc("dc-a");
        failoverConfig.setReplicationPolicy(ReplicationPolicy.IDENTITY);
        failoverConfig.setProbeTimeoutMs(500);

        DcConfig dcA = new DcConfig();
        dcA.setBootstrap("kafka-a:9092");
        dcA.setSourceAlias("dc-a");

        DcConfig dcB = new DcConfig();
        dcB.setBootstrap("kafka-b:9093");
        dcB.setSourceAlias("dc-b");

        Map<String, DcConfig> dcs = new LinkedHashMap<>();
        dcs.put("dc-a", dcA);
        dcs.put("dc-b", dcB);
        failoverConfig.setDcs(dcs);
    }

    // =====================================================================
    // FailoverAutoConfiguration bean creation
    // =====================================================================

    @Nested
    @DisplayName("FailoverAutoConfiguration beans")
    class FailoverBeanTests {

        private FailoverAutoConfiguration autoConfig;

        @BeforeEach
        void setUp() {
            autoConfig = new FailoverAutoConfiguration();
        }

        @Test
        @DisplayName("topicResolver bean is created with failover config")
        void topicResolver_created() {
            TopicResolver resolver = autoConfig.topicResolver(props);
            assertThat(resolver).isNotNull();
        }

        @Test
        @DisplayName("dcHealthProbe bean is created with DC bootstraps")
        void dcHealthProbe_created() {
            DcHealthProbe probe = autoConfig.dcHealthProbe(props);
            assertThat(probe).isNotNull();
            probe.close();
        }

        @Test
        @DisplayName("dcAwareKafkaManager bean is created")
        void dcAwareKafkaManager_created() {
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);
            assertThat(manager).isNotNull();
            assertThat(manager.getActiveDc()).isEqualTo("dc-a");
            assertThat(manager.getAllDcIds()).containsExactlyInAnyOrder("dc-a", "dc-b");
        }

        @Test
        @DisplayName("dcAwareListenerManager bean wires kafkaManager and topicResolver")
        void dcAwareListenerManager_created() {
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);
            KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

            DcAwareListenerManager listenerManager = autoConfig.dcAwareListenerManager(manager, resolver, registry);

            assertThat(listenerManager).isNotNull();
        }

        @Test
        @DisplayName("dcFailoverSupervisor bean is created with all dependencies")
        void dcFailoverSupervisor_created() {
            DcHealthProbe probe = autoConfig.dcHealthProbe(props);
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);

            DcFailoverSupervisor supervisor = autoConfig.dcFailoverSupervisor(
                    probe, manager, mongoTemplate, props);

            assertThat(supervisor).isNotNull();
            probe.close();
        }

        @Test
        @DisplayName("dcAwareKafkaTemplate bean delegates to manager")
        void dcAwareKafkaTemplate_created() {
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);

            var template = autoConfig.dcAwareKafkaTemplate(manager);

            assertThat(template).isNotNull();
            assertThat(template).isInstanceOf(DcAwareKafkaTemplate.class);
        }

        @Test
        @DisplayName("dcAwareConsumerFactory bean delegates to manager")
        void dcAwareConsumerFactory_created() {
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);

            var factory = autoConfig.dcAwareConsumerFactory(manager);

            assertThat(factory).isNotNull();
            assertThat(factory).isInstanceOf(DcAwareConsumerFactory.class);
        }

        @Test
        @DisplayName("dcHealthEndpoint bean is created")
        void dcHealthEndpoint_created() {
            DcHealthProbe probe = mock(DcHealthProbe.class);
            TopicResolver resolver = autoConfig.topicResolver(props);
            DcAwareKafkaManager manager = autoConfig.dcAwareKafkaManager(props, resolver);
            DcFailoverSupervisor supervisor = mock(DcFailoverSupervisor.class);

            DcHealthEndpoint endpoint = autoConfig.dcHealthEndpoint(
                    supervisor, probe, manager, resolver, props);

            assertThat(endpoint).isNotNull();
        }
    }

    // =====================================================================
    // DcAwareConsumerFactory
    // =====================================================================

    @Nested
    @DisplayName("DcAwareConsumerFactory")
    class ConsumerFactoryTests {

        @Test
        @DisplayName("createConsumer delegates to active DC factory")
        @SuppressWarnings("unchecked")
        void createConsumer_delegatesToActiveFactory() {
            DcAwareKafkaManager manager = mock(DcAwareKafkaManager.class);
            ConsumerFactory<String, String> activeFactory = mock(ConsumerFactory.class);
            Consumer<String, String> mockConsumer = mock(Consumer.class);

            when(manager.getActiveConsumerFactory()).thenReturn(activeFactory);
            when(manager.getActiveDc()).thenReturn("dc-a");
            when(activeFactory.createConsumer(anyString(), any(), any(), any(Properties.class)))
                    .thenReturn(mockConsumer);

            var dcFactory = new DcAwareConsumerFactory<String, String>(manager);
            Properties props = new Properties();
            Consumer<String, String> result = dcFactory.createConsumer(
                    "test-group", "prefix", "suffix", props);

            assertThat(result).isSameAs(mockConsumer);
            verify(activeFactory).createConsumer(eq("test-group"), eq("prefix"), eq("suffix"), eq(props));
        }

        @Test
        @DisplayName("isAutoCommit returns false")
        void isAutoCommit_returnsFalse() {
            DcAwareKafkaManager manager = mock(DcAwareKafkaManager.class);
            var dcFactory = new DcAwareConsumerFactory<String, String>(manager);

            assertThat(dcFactory.isAutoCommit()).isFalse();
        }

        @Test
        @DisplayName("getConfigurationProperties delegates to active factory")
        @SuppressWarnings("unchecked")
        void getConfigurationProperties_delegatesToActiveFactory() {
            DcAwareKafkaManager manager = mock(DcAwareKafkaManager.class);
            ConsumerFactory<String, String> activeFactory = mock(ConsumerFactory.class);
            Map<String, Object> expectedProps = Map.of("bootstrap.servers", "kafka-a:9092");

            when(manager.getActiveConsumerFactory()).thenReturn(activeFactory);
            when(activeFactory.getConfigurationProperties()).thenReturn(expectedProps);

            var dcFactory = new DcAwareConsumerFactory<String, String>(manager);
            Map<String, Object> result = dcFactory.getConfigurationProperties();

            assertThat(result).isEqualTo(expectedProps);
        }

        @Test
        @DisplayName("after DC switch, delegates to new active factory")
        @SuppressWarnings("unchecked")
        void afterDcSwitch_delegatesToNewFactory() {
            DcAwareKafkaManager manager = mock(DcAwareKafkaManager.class);
            ConsumerFactory<String, String> factoryA = mock(ConsumerFactory.class);
            ConsumerFactory<String, String> factoryB = mock(ConsumerFactory.class);
            Consumer<String, String> consumerA = mock(Consumer.class);
            Consumer<String, String> consumerB = mock(Consumer.class);

            when(manager.getActiveConsumerFactory())
                    .thenReturn(factoryA)
                    .thenReturn(factoryB);
            when(manager.getActiveDc())
                    .thenReturn("dc-a")
                    .thenReturn("dc-b");
            when(factoryA.createConsumer(anyString(), any(), any(), any(Properties.class))).thenReturn(consumerA);
            when(factoryB.createConsumer(anyString(), any(), any(), any(Properties.class))).thenReturn(consumerB);

            var dcFactory = new DcAwareConsumerFactory<String, String>(manager);

            Consumer<String, String> first = dcFactory.createConsumer("g", "p", "s", new Properties());
            assertThat(first).isSameAs(consumerA);

            Consumer<String, String> second = dcFactory.createConsumer("g", "p", "s", new Properties());
            assertThat(second).isSameAs(consumerB);
        }
    }

    // =====================================================================
    // MongoTransactionAutoConfiguration
    // =====================================================================

    @Nested
    @DisplayName("MongoTransactionAutoConfiguration")
    class MongoTransactionTests {

        @Test
        @DisplayName("mongoTransactionManager creates MongoTransactionManager from factory")
        void mongoTransactionManager_createdFromFactory() {
            var autoConfig = new com.orchestrator.starter.autoconfigure.MongoTransactionAutoConfiguration();
            var dbFactory = mock(org.springframework.data.mongodb.MongoDatabaseFactory.class);

            var txManager = autoConfig.mongoTransactionManager(dbFactory);

            assertThat(txManager).isNotNull();
            assertThat(txManager).isInstanceOf(org.springframework.data.mongodb.MongoTransactionManager.class);
        }

        @Test
        @DisplayName("mongoTransactionTemplate wraps the transaction manager")
        void mongoTransactionTemplate_wrapsManager() {
            var autoConfig = new com.orchestrator.starter.autoconfigure.MongoTransactionAutoConfiguration();
            var txManager = mock(org.springframework.data.mongodb.MongoTransactionManager.class);

            var txTemplate = autoConfig.mongoTransactionTemplate(txManager);

            assertThat(txTemplate).isNotNull();
            assertThat(txTemplate.getTransactionManager()).isSameAs(txManager);
        }
    }
}
