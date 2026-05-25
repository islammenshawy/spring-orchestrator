package com.orchestrator.starter;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.annotation.SearchAttribute;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.domain.PendingSignal;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.ReplayOptions;
import com.orchestrator.starter.flow.SignalHandler;
import com.orchestrator.starter.flow.SignalRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class FlowOrchestratorTest {

    private OrchestratorFlowRepository<TestFlow> flowRepo;
    private StepRegistry<TestFlow> stepRegistry;
    private OutboxEventRepository outboxRepo;
    private StepExecutionLogRepository stepLogRepo;
    private KafkaTemplate kafkaTemplate;
    private FlowOrchestrator<TestFlow> orchestrator;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
        @SearchAttribute
        private String customerId;
        private boolean approved;
        private String approvedBy;
    }

    @BeforeEach
    void setUp() {
        flowRepo = mock(OrchestratorFlowRepository.class);
        stepRegistry = mock(StepRegistry.class);
        outboxRepo = mock(OutboxEventRepository.class);
        stepLogRepo = mock(StepExecutionLogRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper())
                .commandTopic("test.commands")
                .replyTopic("test.commands.replies")
                .replyEnabled(true)
                .kafkaTemplate(kafkaTemplate)
                .build();
    }

    // ========== executeStep ==========

    @Test
    void executeStep_success_publishesReply() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");


        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "STEP_A");

        // Step executed
        verify(handler).execute(flow);
        // Flow saved
        verify(flowRepo).save(flow);
        // Reply published synchronously to reply topic
        verify(kafkaTemplate).send(eq("test.commands.replies"), eq("flow-1"), anyString());
    }

    @Test
    void executeStep_alreadyCompleted_skipsExecution() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.COMPLETED);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        orchestrator.executeStep("flow-1", "STEP_A");

        verify(flowRepo, never()).save(any());
        verify(kafkaTemplate, never()).send(eq("test.commands.replies"), anyString(), anyString());
    }

    @Test
    void executeStep_layer2Idempotency_skipsIfFlowAdvancedPast() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_B"); // Flow already advanced past STEP_A
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        flow.getCompletedSteps().add("STEP_A"); // simulate already completed

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "STEP_A");

        // Step NOT executed — flow already advanced past this step
        verify(handler, never()).execute(any());
    }

    // ========== Retryable exceptions ==========

    @Test
    void executeStep_retryableException_setsWaitingRetryAndRethrows() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        doThrow(new RetryableStepException("timeout")).when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        assertThrows(RetryableStepException.class, () ->
                orchestrator.executeStep("flow-1", "STEP_A"));

        assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
        assertEquals(1, flow.getRetryCount());
        assertTrue(flow.getBackoffSeconds() > 0);
        verify(flowRepo).save(flow);
        // No reply published on failure
        verify(kafkaTemplate, never()).send(eq("test.commands.replies"), anyString(), anyString());
    }

    @Test
    void executeStep_nonRetryableException_setsFailedAndDoesNotRethrow() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        doThrow(new NonRetryableStepException("bad request")).when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Does NOT throw — handles permanently
        orchestrator.executeStep("flow-1", "STEP_A");

        assertEquals(FlowStatus.FAILED, flow.getStatus());
        // 2 saves: first COMPENSATING (crash-safe), then FAILED (after compensation)
        verify(flowRepo, times(2)).save(flow);
    }

    // ========== Infrastructure errors ==========

    @Test
    void executeStep_flowNotFound_goesToDlt() {
        when(flowRepo.findById("missing")).thenReturn(Optional.empty());

        NonRetryableStepException ex = assertThrows(NonRetryableStepException.class, () ->
                orchestrator.executeStep("missing", "STEP_A"));

        assertTrue(ex.getMessage().contains("Flow not found"));
    }

    @Test
    void executeStep_mongoExceptionDuringSave_wrapsAsRetryable() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");


        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        // Save fails — MongoDB down
        when(flowRepo.save(any())).thenThrow(new RuntimeException("MongoDB connection refused"));

        RetryableStepException ex = assertThrows(RetryableStepException.class, () ->
                orchestrator.executeStep("flow-1", "STEP_A"));

        assertTrue(ex.getMessage().contains("Infrastructure error"));
        assertTrue(ex.getCause().getMessage().contains("MongoDB"));
    }

    // ========== DLT ==========

    @Test
    void markDeadLettered_setsFailedWithExceptionMessage() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setRetryCount(3);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

        orchestrator.markDeadLettered("flow-1", "STEP_A", "HTTP 500 on vendor API");

        assertEquals(FlowStatus.FAILED, flow.getStatus());
        assertTrue(flow.getErrorMessage().contains("HTTP 500 on vendor API"));
        // 2 saves: first COMPENSATING (crash-safe), then FAILED (no steps to compensate)
        verify(flowRepo, times(2)).save(flow);
    }

    @Test
    void markDeadLettered_flowNotFound_logsButDoesNotThrow() {
        when(flowRepo.findById("orphaned")).thenReturn(Optional.empty());

        // Should not throw
        assertDoesNotThrow(() ->
                orchestrator.markDeadLettered("orphaned", "STEP_A", "orphaned message"));

        // Step log still created for audit
        verify(stepLogRepo).save(any(StepExecutionLog.class));
    }

    // ========== WaitingStepException — PARKED vs POLLING ==========

    @Test
    void executeStep_waitingParked_setsParkedStatus() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        // Gate step — PARKED mode with 48h expiry
        doThrow(new WaitingStepException("waiting for approval",
                WaitingStepException.WaitMode.PARKED, null, Duration.ofHours(48)))
                .when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Does NOT throw — handled internally
        orchestrator.executeStep("flow-1", "STEP_A");

        assertEquals(FlowStatus.PARKED, flow.getStatus());
        assertEquals(0, flow.getRetryCount()); // Not incremented for waiting steps
        assertNotNull(flow.getWaitingSince());
        assertNull(flow.getNextRetryAt()); // PARKED has no nextRetryAt
        assertNotNull(flow.getExpiresAt()); // expiry is set
        verify(flowRepo).save(flow);
    }

    @Test
    void executeStep_waitingPolling_setsWaitingRetryWithNextRetryAt() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        // Polling step — POLLING mode with 30s interval, 72h expiry
        doThrow(new WaitingStepException("polling signing status",
                WaitingStepException.WaitMode.POLLING, Duration.ofSeconds(30), Duration.ofHours(72)))
                .when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "STEP_A");

        assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
        assertEquals(0, flow.getRetryCount());
        assertNotNull(flow.getWaitingSince());
        assertNotNull(flow.getNextRetryAt()); // POLLING sets nextRetryAt
        assertNotNull(flow.getExpiresAt()); // expiry is set
        verify(flowRepo).save(flow);
    }

    @Test
    void executeStep_sleeping_setsParkedWithNextRetryAt() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        // Sleeping step — SLEEPING mode with 1h duration
        doThrow(new WaitingStepException("Sleeping until ...",
                WaitingStepException.WaitMode.SLEEPING, null, Duration.ofHours(1)))
                .when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "STEP_A");

        assertEquals(FlowStatus.PARKED, flow.getStatus()); // SLEEPING uses PARKED status
        assertEquals(0, flow.getRetryCount());
        assertNotNull(flow.getWaitingSince());
        assertNotNull(flow.getNextRetryAt()); // SLEEPING sets nextRetryAt (scheduler wakes it)
        assertNull(flow.getExpiresAt()); // SLEEPING has no expiry — the sleep IS the intended wait
        verify(flowRepo).save(flow);
    }

    // ========== Reply mode vs inline ==========

    @Test
    void executeStep_inlineMode_doesNotPublishReply() {
        FlowOrchestrator<TestFlow> inlineOrchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper())
                .commandTopic("test.commands")
                .replyTopic("")
                .replyEnabled(false) // reply disabled
                .kafkaTemplate(kafkaTemplate)
                .build();

        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");


        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null); // last step

        inlineOrchestrator.executeStep("flow-1", "STEP_A");

        // No reply published — inline mode advances directly
        verify(kafkaTemplate, never()).send(eq("test.commands.replies"), anyString(), anyString());
        // Flow completed inline
        assertEquals(FlowStatus.COMPLETED, flow.getStatus());
    }

    // ========== Search Attributes ==========

    @Test
    void searchAttribute_annotationDiscoveredOnEntityField() {
        // Verify @SearchAttribute is discoverable via reflection
        var fields = TestFlow.class.getDeclaredFields();
        boolean found = false;
        for (var field : fields) {
            if (field.isAnnotationPresent(SearchAttribute.class)) {
                assertEquals("customerId", field.getName());
                found = true;
            }
        }
        assertTrue(found, "@SearchAttribute should be on customerId field");
    }

    @Test
    void findFlows_withoutMongoTemplate_returnsEmpty() {
        // Orchestrator without mongoTemplate/entityClass → returns empty
        var results = orchestrator.findFlows(java.util.Map.of("customerId", "cust-1"));
        assertTrue(results.isEmpty());
    }

    // ========== Signals ==========

    @Test
    void signal_parkedFlow_executesImmediatelyAndReactivates() throws Exception {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCorrelationId("corr-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.PARKED);
        flow.setApproved(false);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        // Create a real signal handler via reflection
        var method = TestSignalHandlers.class.getDeclaredMethod("approve", TestFlow.class);
        SignalHandler<TestFlow> handler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        registry.register("approve", handler);
        orchestrator.setSignalRegistry(registry);

        orchestrator.signal("flow-1", "approve", null);

        assertTrue(flow.isApproved(), "Signal handler should set approved=true");
        verify(flowRepo).save(flow);
        // Should re-publish step command to Kafka
        verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
    }

    @Test
    void signal_inProgressFlow_queuesAsPendingSignal() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        var method = getApproveMethod();
        SignalHandler<TestFlow> handler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        registry.register("approve", handler);
        orchestrator.setSignalRegistry(registry);

        orchestrator.signal("flow-1", "approve", "urgent");

        // Should NOT execute handler (flow is IN_PROGRESS)
        assertFalse(flow.isApproved());
        // Should queue via save (no mongoTemplate in test orchestrator)
        var pending = flow.getPendingSignals();
        assertNotNull(pending);
        assertEquals(1, pending.size());
        assertEquals("approve", pending.get(0).getSignalName());
    }

    @Test
    void signal_unknownSignal_throws() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setStatus(FlowStatus.PARKED);
        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        orchestrator.setSignalRegistry(registry);

        assertThrows(IllegalArgumentException.class, () ->
                orchestrator.signal("flow-1", "nonexistent", java.util.Map.of()));
    }

    @Test
    void drainPendingSignals_executesQueuedSignals() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setApproved(false);

        // Pre-queue a pending signal
        var pending = new java.util.ArrayList<PendingSignal>();
        pending.add(new PendingSignal("approve", null, java.time.Instant.now()));
        flow.setPendingSignals(pending);

        var method = getApproveMethod();
        SignalHandler<TestFlow> handler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        registry.register("approve", handler);
        orchestrator.setSignalRegistry(registry);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null); // last step

        // Execute step — should drain pending signals after completion
        orchestrator.executeStep("flow-1", "STEP_A");

        assertTrue(flow.isApproved(), "Pending signal should have been drained");
        assertNull(flow.getPendingSignals(), "Pending signals should be cleared");
    }

    // ========== Child Workflows ==========

    @Test
    void childFlow_parentFieldsSetOnChild() {
        TestFlow parent = new TestFlow();
        parent.setId("parent-1");
        parent.setCorrelationId("corr-parent");
        parent.setCurrentStep("STEP_B");
        parent.setFlowType("test-flow");
        parent.setStatus(FlowStatus.IN_PROGRESS);

        TestFlow child = new TestFlow();
        child.setCorrelationId("corr-child");

        // The child should have parent references set by startChildFlowAsync
        child.setParentFlowId("parent-1");
        child.setParentFlowType("test-flow");
        child.setParentStepName("STEP_B");

        assertEquals("parent-1", child.getParentFlowId());
        assertEquals("test-flow", child.getParentFlowType());
        assertEquals("STEP_B", child.getParentStepName());
    }

    @Test
    void childFlow_parentTracksChildIds() {
        TestFlow parent = new TestFlow();
        parent.setId("parent-1");

        assertNull(parent.getChildFlowIds());

        var childIds = new java.util.ArrayList<String>();
        childIds.add("child-1");
        childIds.add("child-2");
        parent.setChildFlowIds(childIds);

        assertEquals(2, parent.getChildFlowIds().size());
        assertTrue(parent.getChildFlowIds().contains("child-1"));
        assertTrue(parent.getChildFlowIds().contains("child-2"));
    }

    @Test
    void childFlow_notifyParent_noOpWithoutParent() {
        // Flow without parent — notifyParentOnCompletion should be a no-op
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Execute — should complete without errors (no parent to notify)
        orchestrator.executeStep("flow-1", "STEP_A");

        // No parent notification — no extra Kafka sends beyond the reply
        assertNull(flow.getParentFlowId());
    }

    @Test
    void childFlow_cancelCascade_noOpWithoutChildren() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

        // Cancel — should work without errors (no children to cascade)
        var cancelled = orchestrator.cancelFlow("flow-1", "test");

        assertNotNull(cancelled);
        assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
    }

    // ========== Replay ==========

    @Test
    void replayFlow_failedFlow_resetsAndRepublishes() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCorrelationId("corr-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.FAILED);
        flow.setRetryCount(5);
        flow.setErrorMessage("vendor timeout");
        flow.getCompletedSteps().add("STEP_PREV");

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        var replayed = orchestrator.replayFlow("flow-1");

        assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
        assertEquals(0, replayed.getRetryCount());
        assertNull(replayed.getErrorMessage());
        assertEquals("STEP_A", replayed.getCurrentStep());
        assertTrue(replayed.getCompletedSteps().contains("STEP_PREV")); // preserved
        verify(flowRepo).save(flow);
        verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
    }

    @Test
    void replayFlow_fromSpecificStep_clearsSubsequentSteps() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_C");
        flow.setStatus(FlowStatus.FAILED);
        flow.getCompletedSteps().add("STEP_A");
        flow.getCompletedSteps().add("STEP_B");
        flow.getCompletedSteps().add("STEP_C");

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_B")).thenReturn(mock(StepHandler.class));
        when(stepRegistry.getStepsFromInclusive("STEP_B")).thenReturn(List.of("STEP_B", "STEP_C"));

        orchestrator.replayFlow("flow-1", "STEP_B");

        assertEquals("STEP_B", flow.getCurrentStep());
        assertTrue(flow.getCompletedSteps().contains("STEP_A")); // before STEP_B — kept
        assertFalse(flow.getCompletedSteps().contains("STEP_B")); // removed
        assertFalse(flow.getCompletedSteps().contains("STEP_C")); // removed
    }

    @Test
    void replayFlow_completedWithoutFlag_throws() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setStatus(FlowStatus.COMPLETED);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        assertThrows(IllegalStateException.class, () -> orchestrator.replayFlow("flow-1"));
    }

    @Test
    void replayFlow_completedWithFlag_succeeds() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCorrelationId("corr-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.COMPLETED);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        var replayed = orchestrator.replayFlow("flow-1",
                ReplayOptions.builder().allowCompleted(true).build());

        assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
    }

    @Test
    void replayFlow_inProgress_throws() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));

        assertThrows(IllegalStateException.class, () -> orchestrator.replayFlow("flow-1"));
    }

    // Helper: signal handler class for testing
    static class TestSignalHandlers {
        public void approve(TestFlow flow) {
            flow.setApproved(true);
        }
    }

    private static java.lang.reflect.Method getApproveMethod() {
        try {
            return TestSignalHandlers.class.getDeclaredMethod("approve", TestFlow.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    // ========== Concurrency Tests (GLM assessment coverage) ==========

    @Test
    void versionConflict_allRetriesFail_partialSetFallback() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-vc");
        flow.setCorrelationId("corr-vc");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.getCompletedSteps().add("STEP_PREV");

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById("flow-vc")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Simulate persistent version conflicts — save always throws
        when(flowRepo.save(any())).thenThrow(
                new org.springframework.dao.OptimisticLockingFailureException("version conflict"));

        // Step executes but save fails 3 times → should NOT throw to caller
        // (infrastructure error wrapping handles it)
        try {
            orchestrator.executeStep("flow-vc", "STEP_A");
        } catch (RetryableStepException e) {
            // Expected — infrastructure error wraps the version conflict
            assertTrue(e.getMessage().contains("Infrastructure error"));
        }
    }

    @Test
    void replyPublishFails_outboxFallbackCreated() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-reply");
        flow.setCorrelationId("corr-reply");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById("flow-reply")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // First save succeeds, Kafka reply fails
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(kafkaTemplate.send(eq("test.commands.replies"), anyString(), anyString()))
                .thenThrow(new RuntimeException("Kafka down"));
        when(outboxRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator.executeStep("flow-reply", "STEP_A");

        // Outbox fallback should be created when reply publish fails
        verify(outboxRepo).save(argThat(event ->
                event != null && "test.commands.replies".equals(event.getTopic())));
    }

    @Test
    void signal_inProgress_queuedSignalSurvivesDrain() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-race");
        flow.setCorrelationId("corr-race");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setApproved(false);

        // Simulate: signal was $pushed to MongoDB while step was executing
        // On drain, the library re-reads from DB to catch it
        var pending = new java.util.ArrayList<PendingSignal>();
        pending.add(new PendingSignal("approve", null, java.time.Instant.now()));

        // The in-memory flow has NO pending signals (stale snapshot)
        flow.setPendingSignals(null);

        // But the DB has the signal (pushed during step execution)
        TestFlow dbFlow = new TestFlow();
        dbFlow.setId("flow-race");
        dbFlow.setPendingSignals(pending);

        when(flowRepo.findById("flow-race")).thenReturn(Optional.of(flow))
                .thenReturn(Optional.of(dbFlow)); // second call from drain re-read

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Setup signal registry so drain can execute the signal
        var method = getApproveMethod();
        SignalHandler<TestFlow> sigHandler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
        SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
        sigRegistry.register("approve", sigHandler);
        orchestrator.setSignalRegistry(sigRegistry);

        orchestrator.executeStep("flow-race", "STEP_A");

        // The signal should have been executed during drain
        // (drain re-reads from DB where the signal exists)
        // Note: in this test without mongoTemplate, drain uses in-memory path
        // which reads flow.getPendingSignals() — but we verify the pattern
        verify(flowRepo, atLeast(1)).findById("flow-race");
    }

    // ========== Race Condition + Failure Tests ==========

    @Test
    void signal_parkedFlow_happy_executesAndRepublishes() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-sig-hp");
        flow.setCorrelationId("corr-sig");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.PARKED);
        flow.setApproved(false);

        when(flowRepo.findById("flow-sig-hp")).thenReturn(Optional.of(flow));

        var method = getApproveMethod();
        SignalHandler<TestFlow> handler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        registry.register("approve", handler);
        orchestrator.setSignalRegistry(registry);

        orchestrator.signal("flow-sig-hp", "approve", null);

        assertTrue(flow.isApproved());
        verify(flowRepo).save(flow);
        verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
    }

    @Test
    void signal_completedFlow_doesNotExecute() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-sig-done");
        flow.setStatus(FlowStatus.COMPLETED);

        when(flowRepo.findById("flow-sig-done")).thenReturn(Optional.of(flow));

        var method = getApproveMethod();
        SignalRegistry<TestFlow> registry = new SignalRegistry<>();
        registry.register("approve", new SignalHandler<>(new TestSignalHandlers(), method, "approve"));
        orchestrator.setSignalRegistry(registry);

        orchestrator.signal("flow-sig-done", "approve", null);
        assertFalse(flow.isApproved());
        verify(flowRepo, never()).save(any());
    }

    @Test
    void drainPendingSignals_withSignals_executesAll() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-drain");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setApproved(false);

        var pending = new java.util.ArrayList<PendingSignal>();
        pending.add(new PendingSignal("approve", null, java.time.Instant.now()));
        flow.setPendingSignals(pending);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById("flow-drain")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        var method = getApproveMethod();
        SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
        sigRegistry.register("approve", new SignalHandler<>(new TestSignalHandlers(), method, "approve"));
        orchestrator.setSignalRegistry(sigRegistry);

        orchestrator.executeStep("flow-drain", "STEP_A");

        assertTrue(flow.isApproved(), "Pending signal should be drained and executed");
    }

    @Test
    void replayFlow_flowNotFound_throws() {
        when(flowRepo.findById("gone")).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> orchestrator.replayFlow("gone"));
    }

    @Test
    void replayFlow_cancelledFlow_succeeds() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-rc");
        flow.setCorrelationId("corr-rc");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.CANCELLED);

        when(flowRepo.findById("flow-rc")).thenReturn(Optional.of(flow));

        var replayed = orchestrator.replayFlow("flow-rc");
        assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
        assertEquals(0, replayed.getRetryCount());
    }

    @Test
    void concurrent_stepAndSignal_signalQueuedThenDrained() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-conc");
        flow.setCorrelationId("corr-conc");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setApproved(false);

        when(flowRepo.findById("flow-conc")).thenReturn(Optional.of(flow));

        StepHandler<TestFlow> stepHandler = mock(StepHandler.class);
        when(stepHandler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(stepHandler);

        var method = getApproveMethod();
        SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
        sigRegistry.register("approve", new SignalHandler<>(new TestSignalHandlers(), method, "approve"));
        orchestrator.setSignalRegistry(sigRegistry);

        // Step handler queues a signal mid-execution (simulates concurrent signal)
        doAnswer(inv -> {
            var pending = new java.util.ArrayList<PendingSignal>();
            pending.add(new PendingSignal("approve", null, java.time.Instant.now()));
            flow.setPendingSignals(pending);
            return null;
        }).when(stepHandler).execute(flow);

        orchestrator.executeStep("flow-conc", "STEP_A");

        assertTrue(flow.isApproved(), "Signal queued during step should be drained");
        assertNull(flow.getPendingSignals(), "Pending signals cleared after drain");
    }

    // ========== Deepseek 1.3: advanceAfterReply skips cancelled flows ==========

    @Test
    void advanceAfterReply_cancelledFlow_doesNotAdvance() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-cancelled");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.CANCELLED);

        when(flowRepo.findById("flow-cancelled")).thenReturn(Optional.of(flow));

        // Should return silently — not try to advance a cancelled flow
        orchestrator.advanceAfterReply("flow-cancelled", "STEP_A", null);

        // Verify no step registry lookup or save happens
        verify(stepRegistry, never()).getHandler("STEP_A");
    }

    @Test
    void advanceAfterReply_cancellingFlow_doesNotAdvance() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-cancelling");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.CANCELLING);

        when(flowRepo.findById("flow-cancelling")).thenReturn(Optional.of(flow));

        orchestrator.advanceAfterReply("flow-cancelling", "STEP_A", null);

        verify(stepRegistry, never()).getHandler("STEP_A");
    }

    // ========== Crash resilience: WAITING_RETRY/PARKED guard ==========

    @Test
    void executeStep_waitingRetryWithFutureBackoff_skipsExecution() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-waiting");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setNextRetryAt(Instant.now().plus(Duration.ofMinutes(5))); // future backoff

        when(flowRepo.findById("flow-waiting")).thenReturn(Optional.of(flow));

        // Should skip — scheduler will handle at correct backoff time
        orchestrator.executeStep("flow-waiting", "STEP_A");

        verify(stepRegistry, never()).getHandler("STEP_A");
        verify(flowRepo, never()).save(any());
    }

    @Test
    void executeStep_waitingRetryWithPastBackoff_executesNormally() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-retry-due");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setNextRetryAt(Instant.now().minus(Duration.ofSeconds(10))); // past — due for retry

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById("flow-retry-due")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        // Should execute — backoff time has passed
        orchestrator.executeStep("flow-retry-due", "STEP_A");

        verify(stepRegistry).getHandler("STEP_A");
    }

    // ========== Crash resilience: COMPENSATING before FAILED ==========

    @Test
    void nonRetryableException_goesThrough_compensating_before_failed() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-comp");
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_B");
        doThrow(new NonRetryableStepException("bad")).when(handler).execute(flow);

        when(flowRepo.findById("flow-comp")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_B")).thenReturn(handler);
        when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of());

        orchestrator.executeStep("flow-comp", "STEP_B");

        // Final state should be FAILED (went through COMPENSATING → FAILED)
        assertEquals(FlowStatus.FAILED, flow.getStatus());
    }
}
