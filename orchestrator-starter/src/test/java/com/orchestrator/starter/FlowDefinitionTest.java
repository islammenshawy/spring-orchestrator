package com.orchestrator.starter;

import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.FlowDefinition;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class FlowDefinitionTest {

    private OrchestratorFlowRepository<TestFlowEntity> rawRepository;
    private FlowTypeRegistry flowTypeRegistry;
    private ConcreteFlowDef flowDef;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "flow_def_test_flows")
    static class TestFlowEntity extends AbstractFlow {
        private String payload;
        private boolean approved;
    }

    static class ConcreteFlowDef extends FlowDefinition<TestFlowEntity> {

        // Expose protected methods for testing

        public void doWaitUntilTrue(TestFlowEntity flow) {
            waitUntil(() -> true, Duration.ofHours(1));
        }

        public void doWaitUntilFalse(TestFlowEntity flow) {
            waitUntil(() -> false, Duration.ofHours(48));
        }

        public void doPollUntilTrue(TestFlowEntity flow) {
            pollUntil(() -> true, Duration.ofSeconds(30), Duration.ofHours(72));
        }

        public void doPollUntilFalse(TestFlowEntity flow) {
            pollUntil(() -> false, Duration.ofSeconds(30), Duration.ofHours(72));
        }

        public void doSleep(TestFlowEntity flow) {
            sleep(flow, Duration.ofHours(1));
        }

        public void doSleepUntilFuture(TestFlowEntity flow, Instant wakeAt) {
            sleepUntil(flow, wakeAt);
        }

        public void doCheckpoint(TestFlowEntity flow) {
            checkpoint(flow);
        }

        public void doCancelFlow(TestFlowEntity flow, String reason) {
            cancelFlow(flow, reason);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        rawRepository = mock(OrchestratorFlowRepository.class);
        flowTypeRegistry = mock(FlowTypeRegistry.class);
        flowDef = new ConcreteFlowDef();

        // Inject rawRepository via reflection (it's @Autowired private)
        Field repoField = FlowDefinition.class.getDeclaredField("rawRepository");
        repoField.setAccessible(true);
        repoField.set(flowDef, rawRepository);

        // Inject flowTypeRegistry via reflection
        Field registryField = FlowDefinition.class.getDeclaredField("flowTypeRegistry");
        registryField.setAccessible(true);
        registryField.set(flowDef, flowTypeRegistry);
    }

    // ========== waitUntil ==========

    @Nested
    class WaitUntilTests {

        @Test
        void conditionTrue_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("wait-true-1");

            assertDoesNotThrow(() -> flowDef.doWaitUntilTrue(flow));
        }

        @Test
        void conditionFalse_throwsWaitingStepExceptionWithParkedMode() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("wait-false-1");

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doWaitUntilFalse(flow));

            assertEquals(WaitingStepException.WaitMode.PARKED, ex.getWaitMode());
            assertTrue(ex.isParked());
            assertNull(ex.getPollInterval(), "PARKED mode should have no poll interval");
            assertEquals(Duration.ofHours(48), ex.getExpiry());
            assertTrue(ex.getMessage().contains("Waiting for condition"));
        }
    }

    // ========== pollUntil ==========

    @Nested
    class PollUntilTests {

        @Test
        void conditionTrue_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("poll-true-1");

            assertDoesNotThrow(() -> flowDef.doPollUntilTrue(flow));
        }

        @Test
        void conditionFalse_throwsWaitingStepExceptionWithPollingMode() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("poll-false-1");

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doPollUntilFalse(flow));

            assertEquals(WaitingStepException.WaitMode.POLLING, ex.getWaitMode());
            assertFalse(ex.isParked());
            assertEquals(Duration.ofSeconds(30), ex.getPollInterval());
            assertEquals(Duration.ofHours(72), ex.getExpiry());
            assertTrue(ex.getMessage().contains("Waiting for condition"));
        }
    }

    // ========== sleep / sleepUntil ==========

    @Nested
    class SleepTests {

        @Test
        void sleep_throwsWaitingStepExceptionWithSleepingMode() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-1");

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doSleep(flow));

            assertEquals(WaitingStepException.WaitMode.SLEEPING, ex.getWaitMode());
            assertNull(ex.getPollInterval(), "SLEEPING mode should have no poll interval");
            assertNotNull(ex.getExpiry());
            // sleepUntil sets the flow's sleepUntil field and calls checkpoint
            assertNotNull(flow.getSleepUntil(), "sleep() should set sleepUntil on flow");
            verify(rawRepository).save(flow);
        }

        @Test
        void sleepUntil_futureInstant_throwsWaitingStepException() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-future-1");
            Instant wakeAt = Instant.now().plus(Duration.ofHours(2));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doSleepUntilFuture(flow, wakeAt));

            assertEquals(WaitingStepException.WaitMode.SLEEPING, ex.getWaitMode());
            assertEquals(wakeAt, flow.getSleepUntil());
            verify(rawRepository).save(flow);
        }

        @Test
        void sleepUntil_pastInstant_flowAlreadySlept_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-past-1");
            Instant pastWake = Instant.now().minus(Duration.ofMinutes(5));
            flow.setSleepUntil(pastWake); // already set from previous call

            // Re-delivery after timer fired: sleep is done, should continue
            assertDoesNotThrow(() -> flowDef.doSleepUntilFuture(flow, pastWake));
            // No checkpoint on re-delivery pass-through
            verify(rawRepository, never()).save(any());
        }

        @Test
        void sleepUntil_sleepUntilNotYetSet_futureTarget_parks() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-first-call-1");
            assertNull(flow.getSleepUntil());

            Instant wakeAt = Instant.now().plus(Duration.ofMinutes(30));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doSleepUntilFuture(flow, wakeAt));

            assertEquals(WaitingStepException.WaitMode.SLEEPING, ex.getWaitMode());
            assertEquals(wakeAt, flow.getSleepUntil());
            assertTrue(ex.getMessage().contains("Sleeping until"));
        }
    }

    // ========== checkpoint ==========

    @Nested
    class CheckpointTests {

        @Test
        void checkpoint_callsRepositorySave() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("chk-1");
            flow.setPayload("important-data");

            flowDef.doCheckpoint(flow);

            verify(rawRepository).save(flow);
        }

        @Test
        void checkpoint_calledMultipleTimes_savesEachTime() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("chk-multi-1");

            flowDef.doCheckpoint(flow);
            flowDef.doCheckpoint(flow);
            flowDef.doCheckpoint(flow);

            verify(rawRepository, times(3)).save(flow);
        }
    }

    // ========== cancelFlow ==========

    @Nested
    class CancelFlowTests {

        @Test
        void cancelFlow_delegatesToOrchestrator() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("cancel-1");

            FlowTypeDescriptor descriptor = mock(FlowTypeDescriptor.class);
            FlowOrchestrator orchestrator = mock(FlowOrchestrator.class);
            when(descriptor.getOrchestrator()).thenReturn(orchestrator);
            when(flowTypeRegistry.getByEntityClass(flow.getClass())).thenReturn(descriptor);

            flowDef.doCancelFlow(flow, "user requested");

            verify(orchestrator).cancelFlow("cancel-1", "user requested");
        }

        @Test
        void cancelFlow_noFlowTypeRegistry_throwsIllegalState() throws Exception {
            // Set flowTypeRegistry to null
            Field registryField = FlowDefinition.class.getDeclaredField("flowTypeRegistry");
            registryField.setAccessible(true);
            registryField.set(flowDef, null);

            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("cancel-noreg-1");

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> flowDef.doCancelFlow(flow, "reason"));

            assertTrue(ex.getMessage().contains("FlowTypeRegistry not available"));
        }

        @Test
        void cancelFlow_noDescriptorForEntityClass_throwsIllegalState() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("cancel-nodesc-1");

            when(flowTypeRegistry.getByEntityClass(flow.getClass())).thenReturn(null);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> flowDef.doCancelFlow(flow, "reason"));

            assertTrue(ex.getMessage().contains("No flow type for"));
        }
    }
}
