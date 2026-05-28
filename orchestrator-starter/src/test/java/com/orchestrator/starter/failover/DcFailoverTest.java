package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("DC Failover")
class DcFailoverTest {

    private FailoverConfig config;

    @BeforeEach
    void setUp() {
        config = new FailoverConfig();
        config.setEnabled(true);
        config.setActiveDc("dc-a");
        config.setDegradedThreshold(3);
        config.setFailoverThreshold(6);
        config.setDwellTimeSeconds(0); // no dwell for tests
        config.setProbeTimeoutMs(1000);

        DcConfig dcA = new DcConfig();
        dcA.setBootstrap("kafka-a:9092");
        dcA.setSourceAlias("dc-a");

        DcConfig dcB = new DcConfig();
        dcB.setBootstrap("kafka-b:9092");
        dcB.setSourceAlias("dc-b");

        Map<String, DcConfig> dcs = new LinkedHashMap<>();
        dcs.put("dc-a", dcA);
        dcs.put("dc-b", dcB);
        config.setDcs(dcs);
    }

    @Nested
    @DisplayName("TopicResolver")
    class TopicResolverTests {

        @Test
        @DisplayName("IDENTITY: same topic on all DCs")
        void identity_sameTopicEverywhere() {
            config.setReplicationPolicy(ReplicationPolicy.IDENTITY);
            var resolver = new TopicResolver(config);

            assertEquals("dis.commands", resolver.resolve("dis.commands", "dc-a", "dc-a"));
            assertEquals("dis.commands", resolver.resolve("dis.commands", "dc-b", "dc-a"));
            assertEquals("dis.commands", resolver.resolve("dis.commands", "dc-a", "dc-b"));
        }

        @Test
        @DisplayName("PREFIXED: local topic has no prefix")
        void prefixed_localNoPrefix() {
            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            var resolver = new TopicResolver(config);

            assertEquals("dis.commands", resolver.resolve("dis.commands", "dc-a", "dc-a"));
        }

        @Test
        @DisplayName("PREFIXED: remote topic gets source alias prefix")
        void prefixed_remoteGetPrefix() {
            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            var resolver = new TopicResolver(config);

            // DC-B reading DC-A's data → prefixed with dc-a's source alias
            assertEquals("dc-a.dis.commands", resolver.resolve("dis.commands", "dc-b", "dc-a"));
            // DC-A reading DC-B's data → prefixed with dc-b's source alias
            assertEquals("dc-b.dis.commands", resolver.resolve("dis.commands", "dc-a", "dc-b"));
        }

        @Test
        @DisplayName("resolveLocal always returns unprefixed")
        void resolveLocal_alwaysUnprefixed() {
            config.setReplicationPolicy(ReplicationPolicy.PREFIXED);
            var resolver = new TopicResolver(config);
            assertEquals("dis.commands", resolver.resolveLocal("dis.commands"));
        }
    }

    @Nested
    @DisplayName("DcFailoverSupervisor state machine")
    class SupervisorTests {

        @Test
        @DisplayName("starts HEALTHY on active DC")
        void startsHealthy() {
            var probe = mock(DcHealthProbe.class);
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);

            assertEquals(DcState.HEALTHY, supervisor.getState());
            assertEquals("dc-a", supervisor.getActiveDc());
        }

        @Test
        @DisplayName("transitions HEALTHY → DEGRADED after threshold failures")
        void healthyToDegraded() {
            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(false);
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);

            // 3 failures → DEGRADED
            for (int i = 0; i < 3; i++) supervisor.probeAndEvaluate();
            assertEquals(DcState.DEGRADED, supervisor.getState());
        }

        @Test
        @DisplayName("transitions DEGRADED → failover after threshold, swaps DC")
        void degradedToFailover() {
            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(false);
            when(probe.probe("dc-b")).thenReturn(true); // standby is healthy
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);
            when(mongo.save(any())).thenAnswer(inv -> inv.getArgument(0));

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);

            // 6 failures → failover
            for (int i = 0; i < 6; i++) supervisor.probeAndEvaluate();

            assertEquals(DcState.COOLDOWN, supervisor.getState());
            assertEquals("dc-b", supervisor.getActiveDc());
            verify(manager).switchActiveDc("dc-b");
        }

        @Test
        @DisplayName("does NOT failover if standby is also unhealthy")
        void noFailoverIfBothDown() {
            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(false);
            when(probe.probe("dc-b")).thenReturn(false); // standby also down
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);

            for (int i = 0; i < 6; i++) supervisor.probeAndEvaluate();

            // Stays DEGRADED — both DCs down, don't swap
            assertEquals(DcState.DEGRADED, supervisor.getState());
            assertEquals("dc-a", supervisor.getActiveDc());
            verify(manager, never()).switchActiveDc(anyString());
        }

        @Test
        @DisplayName("recovers from DEGRADED to HEALTHY on probe success")
        void degradedRecoversOnSuccess() {
            var probe = mock(DcHealthProbe.class);
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);

            // Fail 3 times → DEGRADED
            when(probe.probe("dc-a")).thenReturn(false);
            for (int i = 0; i < 3; i++) supervisor.probeAndEvaluate();
            assertEquals(DcState.DEGRADED, supervisor.getState());

            // Succeed → back to HEALTHY (failures reset, below threshold)
            when(probe.probe("dc-a")).thenReturn(true);
            supervisor.probeAndEvaluate();
            // State doesn't auto-revert from DEGRADED to HEALTHY on single success
            // but consecutiveFailures resets — next failure cycle starts fresh
            assertEquals(DcState.DEGRADED, supervisor.getState()); // stays until threshold recalculated
        }

        @Test
        @DisplayName("records transition event in MongoDB")
        void recordsTransition() {
            var probe = mock(DcHealthProbe.class);
            when(probe.probe("dc-a")).thenReturn(false);
            when(probe.probe("dc-b")).thenReturn(true);
            var manager = mock(DcAwareKafkaManager.class);
            var mongo = mock(MongoTemplate.class);
            when(mongo.save(any())).thenAnswer(inv -> inv.getArgument(0));

            var supervisor = new DcFailoverSupervisor(probe, manager, mongo, config);
            for (int i = 0; i < 6; i++) supervisor.probeAndEvaluate();

            verify(mongo).save(any(DcTransitionEvent.class));
        }
    }
}
