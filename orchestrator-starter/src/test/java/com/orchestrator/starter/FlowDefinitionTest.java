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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

        public String doStartChildFlow(TestFlowEntity flow, Class<? extends FlowDefinition> childClass,
                                        TestFlowEntity child, Duration expiry) {
            return startChildFlow(flow, childClass, child, expiry);
        }

        public String doStartChildFlowAsync(TestFlowEntity flow, Class<? extends FlowDefinition> childClass,
                                             TestFlowEntity child, Duration expiry) {
            return startChildFlowAsync(flow, childClass, child, expiry);
        }

        public void doAwaitChildren(TestFlowEntity flow, Duration expiry) {
            awaitChildren(flow, expiry);
        }

        public void doWaitUntilDynamic(TestFlowEntity flow, java.util.function.BooleanSupplier condition, Duration expiry) {
            waitUntil(condition, expiry);
        }

        public void doPollUntilDynamic(TestFlowEntity flow, java.util.function.BooleanSupplier condition,
                                        Duration pollInterval, Duration expiry) {
            pollUntil(condition, pollInterval, expiry);
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

    // ========== startChildFlowAsync ==========

    /** Dummy child flow definition class used in child-flow tests. */
    static class ChildFlowDef extends FlowDefinition<TestFlowEntity> {}

    @Nested
    class StartChildFlowAsyncTests {

        @Test
        void noFlowTypeRegistry_throwsIllegalState() throws Exception {
            Field registryField = FlowDefinition.class.getDeclaredField("flowTypeRegistry");
            registryField.setAccessible(true);
            registryField.set(flowDef, null);

            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-noreg-1");
            TestFlowEntity child = new TestFlowEntity();

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1)));

            assertTrue(ex.getMessage().contains("FlowTypeRegistry not available"));
        }

        @Test
        void noDescriptorForChildClass_throwsIllegalArgument() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-nodesc-1");
            TestFlowEntity child = new TestFlowEntity();

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(null);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1)));

            assertTrue(ex.getMessage().contains("No flow type registered for"));
        }

        @Test
        void newChild_nullCorrelationId_autoGeneratesDeterministicId() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-auto-corr-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();
            // correlationId is null by default

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-started-1");
            when(childOrch.startFlow(any())).thenReturn(started);

            String childId = flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("child-started-1", childId);
            // Auto-generated correlationId: parentId:child:childType:0
            assertEquals("parent-auto-corr-1:child:childType:0", child.getCorrelationId());
        }

        @Test
        void newChild_correlationIdNotStartingWithParentId_prefixed() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-prefix-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();
            child.setCorrelationId("custom-corr");

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-started-2");
            when(childOrch.startFlow(any())).thenReturn(started);

            flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("parent-prefix-1:child:custom-corr", child.getCorrelationId());
        }

        @Test
        void newChild_correlationIdAlreadyStartsWithParentId_keptAsIs() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-keep-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();
            child.setCorrelationId("parent-keep-1:my-custom");

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId("parent-keep-1:my-custom")).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-started-3");
            when(childOrch.startFlow(any())).thenReturn(started);

            flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("parent-keep-1:my-custom", child.getCorrelationId());
        }

        @Test
        void newChild_setsParentReferencesAndTracksId() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-refs-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step2");

            TestFlowEntity child = new TestFlowEntity();

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-started-4");
            when(childOrch.startFlow(any())).thenReturn(started);

            String childId = flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            // Verify parent references set on child
            assertEquals("parent-refs-1", child.getParentFlowId());
            assertEquals("parentType", child.getParentFlowType());
            assertEquals("step2", child.getParentStepName());

            // Verify child ID tracked on parent
            assertNotNull(parent.getChildFlowIds());
            assertTrue(parent.getChildFlowIds().contains("child-started-4"));

            // Verify checkpoint called
            verify(rawRepository).save(parent);

            assertEquals("child-started-4", childId);
        }

        @Test
        void existingChild_found_returnsExistingIdWithoutStarting() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-idem-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();
            // correlationId null → auto-generate

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);

            // Simulate existing child in DB
            TestFlowEntity existingChild = new TestFlowEntity();
            existingChild.setId("existing-child-1");
            when(childRepo.findByCorrelationId("parent-idem-1:child:childType:0"))
                    .thenReturn(Optional.of(existingChild));

            String childId = flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("existing-child-1", childId);
            // Should NOT call startFlow
            verify(childOrch, never()).startFlow(any());
            // Should track the existing child ID
            assertTrue(parent.getChildFlowIds().contains("existing-child-1"));
        }

        @Test
        void existingChild_alreadyInChildIds_doesNotDuplicate() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-nodup-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");
            List<String> existingIds = new ArrayList<>();
            existingIds.add("existing-child-2");
            parent.setChildFlowIds(existingIds);

            TestFlowEntity child = new TestFlowEntity();

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);

            TestFlowEntity existingChild = new TestFlowEntity();
            existingChild.setId("existing-child-2");
            // childIds.size() is 1, so auto-generated correlationId uses index 1
            when(childRepo.findByCorrelationId("parent-nodup-1:child:childType:1"))
                    .thenReturn(Optional.of(existingChild));

            String childId = flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("existing-child-2", childId);
            // Should still have exactly one entry (no duplicate)
            assertEquals(1, parent.getChildFlowIds().stream()
                    .filter(id -> id.equals("existing-child-2")).count());
        }

        @Test
        void childFlowIds_initializedFromNull() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-null-ids-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");
            assertNull(parent.getChildFlowIds());

            TestFlowEntity child = new TestFlowEntity();

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-init-1");
            when(childOrch.startFlow(any())).thenReturn(started);

            flowDef.doStartChildFlowAsync(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertNotNull(parent.getChildFlowIds());
            assertEquals(1, parent.getChildFlowIds().size());
            assertEquals("child-init-1", parent.getChildFlowIds().get(0));
        }
    }

    // ========== startChildFlow (blocking) ==========

    @Nested
    class StartChildFlowTests {

        @Test
        void startChildFlow_childCompletesImmediately_returnsChildId() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-blocking-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-blocking-1");
            started.setStatus(FlowStatus.COMPLETED);
            when(childOrch.startFlow(any())).thenReturn(started);

            // awaitChildren will look up children — need to return the started child as COMPLETED
            when(childRepo.findById("child-blocking-1")).thenReturn(Optional.of(started));
            when(flowTypeRegistry.getAll()).thenReturn(List.of(childDesc));

            String childId = flowDef.doStartChildFlow(parent, ChildFlowDef.class, child, Duration.ofHours(1));

            assertEquals("child-blocking-1", childId);
        }

        @Test
        void startChildFlow_childStillRunning_throwsWaitingStepException() {
            TestFlowEntity parent = new TestFlowEntity();
            parent.setId("parent-blocking-wait-1");
            parent.setFlowType("parentType");
            parent.setCurrentStep("step1");

            TestFlowEntity child = new TestFlowEntity();

            FlowTypeDescriptor childDesc = mock(FlowTypeDescriptor.class);
            FlowOrchestrator childOrch = mock(FlowOrchestrator.class);
            OrchestratorFlowRepository childRepo = mock(OrchestratorFlowRepository.class);

            when(flowTypeRegistry.getByFlowDefinitionClass(ChildFlowDef.class)).thenReturn(childDesc);
            when(childDesc.getFlowType()).thenReturn("childType");
            when(childDesc.getRepository()).thenReturn(childRepo);
            when(childDesc.getOrchestrator()).thenReturn(childOrch);
            when(childRepo.findByCorrelationId(anyString())).thenReturn(Optional.empty());

            TestFlowEntity started = new TestFlowEntity();
            started.setId("child-blocking-running-1");
            started.setStatus(FlowStatus.IN_PROGRESS);
            when(childOrch.startFlow(any())).thenReturn(started);

            when(childRepo.findById("child-blocking-running-1")).thenReturn(Optional.of(started));
            when(flowTypeRegistry.getAll()).thenReturn(List.of(childDesc));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doStartChildFlow(parent, ChildFlowDef.class, child, Duration.ofHours(1)));

            assertEquals(WaitingStepException.WaitMode.PARKED, ex.getWaitMode());
            assertTrue(ex.getMessage().contains("Waiting for child"));
        }
    }

    // ========== awaitChildren ==========

    @Nested
    class AwaitChildrenTests {

        @Test
        void nullChildIds_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-null-1");
            flow.setChildFlowIds(null);

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void emptyChildIds_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-empty-1");
            flow.setChildFlowIds(new ArrayList<>());

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void allChildrenCompleted_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-done-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-c1");
            ids.add("child-c2");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity completedChild1 = new TestFlowEntity();
            completedChild1.setId("child-c1");
            completedChild1.setStatus(FlowStatus.COMPLETED);

            TestFlowEntity completedChild2 = new TestFlowEntity();
            completedChild2.setId("child-c2");
            completedChild2.setStatus(FlowStatus.COMPLETED);

            when(repo.findById("child-c1")).thenReturn(Optional.of(completedChild1));
            when(repo.findById("child-c2")).thenReturn(Optional.of(completedChild2));

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void childFailed_treatedAsTerminal_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-failed-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-f1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity failedChild = new TestFlowEntity();
            failedChild.setId("child-f1");
            failedChild.setStatus(FlowStatus.FAILED);

            when(repo.findById("child-f1")).thenReturn(Optional.of(failedChild));

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void childCancelled_treatedAsTerminal_returnsNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-cancel-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-x1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity cancelledChild = new TestFlowEntity();
            cancelledChild.setId("child-x1");
            cancelledChild.setStatus(FlowStatus.CANCELLED);

            when(repo.findById("child-x1")).thenReturn(Optional.of(cancelledChild));

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void childStillInProgress_throwsWaitingStepException() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-inprog-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-ip1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity inProgressChild = new TestFlowEntity();
            inProgressChild.setId("child-ip1");
            inProgressChild.setStatus(FlowStatus.IN_PROGRESS);

            when(repo.findById("child-ip1")).thenReturn(Optional.of(inProgressChild));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doAwaitChildren(flow, Duration.ofHours(2)));

            assertEquals(WaitingStepException.WaitMode.PARKED, ex.getWaitMode());
            assertTrue(ex.getMessage().contains("child-ip1"));
            assertTrue(ex.getMessage().contains("IN_PROGRESS"));
            assertEquals(Duration.ofHours(2), ex.getExpiry());
        }

        @Test
        void childPending_throwsWaitingStepException() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-pending-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-p1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity pendingChild = new TestFlowEntity();
            pendingChild.setId("child-p1");
            pendingChild.setStatus(FlowStatus.PENDING);

            when(repo.findById("child-p1")).thenReturn(Optional.of(pendingChild));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));

            assertEquals(WaitingStepException.WaitMode.PARKED, ex.getWaitMode());
        }

        @Test
        void childNotFoundInAnyRepo_treatedAsNull_continuesWithoutException() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-notfound-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-missing-1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            when(repo.findById("child-missing-1")).thenReturn(Optional.empty());

            // findChildFlow returns null when not found, awaitChildren skips null children
            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }

        @Test
        void multipleChildren_firstCompletedSecondInProgress_throwsForSecond() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-multi-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-m1");
            ids.add("child-m2");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor desc = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(desc.getRepository()).thenReturn(repo);
            when(flowTypeRegistry.getAll()).thenReturn(List.of(desc));

            TestFlowEntity completedChild = new TestFlowEntity();
            completedChild.setId("child-m1");
            completedChild.setStatus(FlowStatus.COMPLETED);

            TestFlowEntity runningChild = new TestFlowEntity();
            runningChild.setId("child-m2");
            runningChild.setStatus(FlowStatus.IN_PROGRESS);

            when(repo.findById("child-m1")).thenReturn(Optional.of(completedChild));
            when(repo.findById("child-m2")).thenReturn(Optional.of(runningChild));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));

            assertTrue(ex.getMessage().contains("child-m2"));
        }

        @Test
        void repoWithNullRepository_skippedDuringChildSearch() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("await-nullrepo-1");
            List<String> ids = new ArrayList<>();
            ids.add("child-nr1");
            flow.setChildFlowIds(ids);

            FlowTypeDescriptor descNoRepo = mock(FlowTypeDescriptor.class);
            when(descNoRepo.getRepository()).thenReturn(null);

            FlowTypeDescriptor descWithRepo = mock(FlowTypeDescriptor.class);
            OrchestratorFlowRepository repo = mock(OrchestratorFlowRepository.class);
            when(descWithRepo.getRepository()).thenReturn(repo);

            TestFlowEntity completedChild = new TestFlowEntity();
            completedChild.setId("child-nr1");
            completedChild.setStatus(FlowStatus.COMPLETED);
            when(repo.findById("child-nr1")).thenReturn(Optional.of(completedChild));

            when(flowTypeRegistry.getAll()).thenReturn(List.of(descNoRepo, descWithRepo));

            assertDoesNotThrow(() -> flowDef.doAwaitChildren(flow, Duration.ofHours(1)));
        }
    }

    // ========== sleepUntil edge cases ==========

    @Nested
    class SleepUntilEdgeCaseTests {

        @Test
        void sleepUntil_sleepAlreadySetButStillInFuture_throwsAgain() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-future-still-1");
            Instant futureWake = Instant.now().plus(Duration.ofHours(5));
            flow.setSleepUntil(futureWake);

            // Timer hasn't fired yet (sleepUntil is still in the future),
            // so it should re-park with the new wakeAt
            Instant newWakeAt = Instant.now().plus(Duration.ofHours(3));

            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doSleepUntilFuture(flow, newWakeAt));

            assertEquals(WaitingStepException.WaitMode.SLEEPING, ex.getWaitMode());
            // The sleepUntil should be updated to the new wakeAt
            assertEquals(newWakeAt, flow.getSleepUntil());
            verify(rawRepository).save(flow);
        }

        @Test
        void sleepUntil_exactlyAtWakeTime_continuesNormally() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("sleep-exact-1");
            // Set sleepUntil to "now" — !Instant.now().isBefore(sleepUntil) is true
            Instant nowInstant = Instant.now();
            flow.setSleepUntil(nowInstant);

            // Re-delivery at or after the wake time — should continue
            assertDoesNotThrow(() -> flowDef.doSleepUntilFuture(flow, nowInstant));
            verify(rawRepository, never()).save(any());
        }
    }

    // ========== waitUntil / pollUntil edge cases ==========

    @Nested
    class WaitPollEdgeCaseTests {

        @Test
        void waitUntil_dynamicConditionBasedOnFlowField() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("wait-dynamic-1");
            flow.setApproved(false);

            // First call: condition false -> throws
            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doWaitUntilDynamic(flow, flow::isApproved, Duration.ofHours(24)));

            assertEquals(WaitingStepException.WaitMode.PARKED, ex.getWaitMode());

            // Simulate external approval
            flow.setApproved(true);

            // Second call: condition true -> returns normally
            assertDoesNotThrow(() -> flowDef.doWaitUntilDynamic(flow, flow::isApproved, Duration.ofHours(24)));
        }

        @Test
        void pollUntil_dynamicConditionBasedOnFlowField() {
            TestFlowEntity flow = new TestFlowEntity();
            flow.setId("poll-dynamic-1");
            flow.setPayload(null);

            // Condition false -> throws
            WaitingStepException ex = assertThrows(WaitingStepException.class,
                    () -> flowDef.doPollUntilDynamic(flow, () -> flow.getPayload() != null,
                            Duration.ofSeconds(10), Duration.ofHours(1)));

            assertEquals(WaitingStepException.WaitMode.POLLING, ex.getWaitMode());
            assertEquals(Duration.ofSeconds(10), ex.getPollInterval());

            // Simulate data arrival
            flow.setPayload("received");

            assertDoesNotThrow(() -> flowDef.doPollUntilDynamic(flow, () -> flow.getPayload() != null,
                    Duration.ofSeconds(10), Duration.ofHours(1)));
        }
    }
}
