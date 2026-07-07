package com.orchestrator.starter;

import com.mongodb.client.result.UpdateResult;
import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.domain.PendingSignal;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.*;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FlowOrchestrator — focuses on uncovered paths.
 * All dependencies are mocked; no MongoDB or Kafka required.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({"unchecked", "rawtypes"})
class FlowOrchestratorUnitTest {

    @Mock private OrchestratorFlowRepository<TestFlow> flowRepo;
    @Mock private StepRegistry<TestFlow> stepRegistry;
    @Mock private OutboxEventRepository outboxRepo;
    @Mock private StepExecutionLogRepository stepLogRepo;
    @Mock private KafkaTemplate kafkaTemplate;
    @Mock private MongoTemplate mongoTemplate;

    private ObjectMapper objectMapper;

    /** Orchestrator WITHOUT mongoTemplate (inline/fallback mode). */
    private FlowOrchestrator<TestFlow> orchestrator;

    /** Orchestrator WITH mongoTemplate (CAS mode). */
    private FlowOrchestrator<TestFlow> casOrchestrator;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "unit_test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
        private boolean approved;
        private String approvedBy;
    }

    static class TestSignalHandlers {
        public void approve(TestFlow flow) {
            flow.setApproved(true);
        }

        public void approveWithPayload(TestFlow flow, String approver) {
            flow.setApproved(true);
            flow.setApprovedBy(approver);
        }
    }

    private static java.lang.reflect.Method getApproveMethod() {
        try {
            return TestSignalHandlers.class.getDeclaredMethod("approve", TestFlow.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(outboxRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Inline orchestrator (no mongoTemplate)
        orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(objectMapper)
                .commandTopic("test.commands")
                .replyTopic("test.replies")
                .replyEnabled(false)
                .kafkaTemplate(kafkaTemplate)
                .build();

        // CAS orchestrator (with mongoTemplate)
        casOrchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(objectMapper)
                .commandTopic("test.commands")
                .replyTopic("test.replies")
                .replyEnabled(false)
                .kafkaTemplate(kafkaTemplate)
                .build();
        casOrchestrator.setMongoTemplate(mongoTemplate);
        casOrchestrator.setEntityClass(TestFlow.class);
    }

    private TestFlow newFlow(String id, FlowStatus status, String currentStep) {
        TestFlow f = new TestFlow();
        f.setId(id);
        f.setCurrentStep(currentStep);
        f.setStatus(status);
        f.setVersion(1L);
        return f;
    }

    private static UpdateResult ack(long modifiedCount) {
        return UpdateResult.acknowledged(modifiedCount, modifiedCount, null);
    }

    // ====================================================================
    // cancelFlow()
    // ====================================================================

    @Nested
    @DisplayName("cancelFlow")
    class CancelFlowTests {

        @Test
        @DisplayName("CAS success path — transitions to CANCELLING then CANCELLED")
        void casSuccess_cancelFlowTransitions() {
            TestFlow flow = newFlow("cf-1", FlowStatus.IN_PROGRESS, "STEP_B");
            flow.getCompletedSteps().add("STEP_A");

            // CAS succeeds — casUpdateStatus sets errorMessage via $set, then findById returns flow
            // We need the flow returned by findById to have errorMessage set
            TestFlow casResult = newFlow("cf-1", FlowStatus.CANCELLING, "STEP_B");
            casResult.getCompletedSteps().add("STEP_A");
            casResult.setErrorMessage("CANCELLED: testing");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("cf-1")).thenReturn(Optional.of(casResult));
            when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of("STEP_A"));
            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            TestFlow cancelled = casOrchestrator.cancelFlow("cf-1", "testing");

            assertNotNull(cancelled);
            assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
            assertTrue(cancelled.getErrorMessage().contains("CANCELLED"));
            assertTrue(cancelled.getErrorMessage().contains("testing"));
        }

        @Test
        @DisplayName("CAS failure — flow already in terminal state returns null")
        void casFailure_nonCancellableStatus() {
            TestFlow flow = newFlow("cf-2", FlowStatus.COMPLETED, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("cf-2")).thenReturn(Optional.of(flow));

            TestFlow result = casOrchestrator.cancelFlow("cf-2", "too late");

            assertNull(result);
        }

        @Test
        @DisplayName("CAS failure — flow not found returns null")
        void casFailure_flowNotFound() {
            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("cf-3")).thenReturn(Optional.empty());

            TestFlow result = casOrchestrator.cancelFlow("cf-3", "gone");

            assertNull(result);
        }

        @Test
        @DisplayName("Fallback path (no mongoTemplate) — inline cancel")
        void fallbackPath_inlineCancel() {
            TestFlow flow = newFlow("cf-4", FlowStatus.PARKED, "STEP_A");
            when(flowRepo.findById("cf-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            TestFlow cancelled = orchestrator.cancelFlow("cf-4", "user requested");

            assertNotNull(cancelled);
            assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
            verify(flowRepo, atLeastOnce()).save(flow);
        }

        @Test
        @DisplayName("CAS failure fallback — flow in cancellable state uses inline save")
        void casFailureFallback_cancellableState() {
            TestFlow flow = newFlow("cf-5", FlowStatus.WAITING_RETRY, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("cf-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            TestFlow cancelled = casOrchestrator.cancelFlow("cf-5", "fallback test");

            assertNotNull(cancelled);
            assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
        }

        @Test
        @DisplayName("Cancel with completed steps runs cancel handlers in reverse")
        void cancel_runsHandlersInReverse() {
            TestFlow flow = newFlow("cf-6", FlowStatus.IN_PROGRESS, "STEP_C");
            flow.getCompletedSteps().addAll(Set.of("STEP_A", "STEP_B"));

            when(flowRepo.findById("cf-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_C"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A", "STEP_B")));

            MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
            MethodStepAdapter<TestFlow> adapterB = mock(MethodStepAdapter.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
            when(stepRegistry.getHandler("STEP_B")).thenReturn(adapterB);

            orchestrator.cancelFlow("cf-6", "test");

            // Verify cancel called in reverse order (B before A)
            var inOrder = inOrder(adapterB, adapterA);
            inOrder.verify(adapterB).cancel(flow);
            inOrder.verify(adapterA).cancel(flow);
        }

        @Test
        @DisplayName("Cancel includes current step if in completedSteps set")
        void cancel_includesCurrentStepIfCompleted() {
            TestFlow flow = newFlow("cf-7", FlowStatus.IN_PROGRESS, "STEP_B");
            flow.getCompletedSteps().addAll(Set.of("STEP_A", "STEP_B"));

            when(flowRepo.findById("cf-7")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
            MethodStepAdapter<TestFlow> adapterB = mock(MethodStepAdapter.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
            when(stepRegistry.getHandler("STEP_B")).thenReturn(adapterB);

            orchestrator.cancelFlow("cf-7", "test");

            verify(adapterB).cancel(flow);
            verify(adapterA).cancel(flow);
        }

        @Test
        @DisplayName("Cancel handler failure does not prevent CANCELLED status")
        void cancel_handlerFailure_stillCancels() {
            TestFlow flow = newFlow("cf-8", FlowStatus.IN_PROGRESS, "STEP_B");
            flow.getCompletedSteps().add("STEP_A");

            when(flowRepo.findById("cf-8")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            doThrow(new RuntimeException("cancel blew up")).when(adapter).cancel(flow);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapter);

            TestFlow cancelled = orchestrator.cancelFlow("cf-8", "test");

            assertNotNull(cancelled);
            assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
        }

        @Test
        @DisplayName("Cancel with no completed steps — empty cancellation")
        void cancel_noCompletedSteps() {
            TestFlow flow = newFlow("cf-9", FlowStatus.PENDING, "STEP_A");

            when(flowRepo.findById("cf-9")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            TestFlow cancelled = orchestrator.cancelFlow("cf-9", "test");

            assertNotNull(cancelled);
            assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
        }
    }

    // ====================================================================
    // replayFlow()
    // ====================================================================

    @Nested
    @DisplayName("replayFlow")
    class ReplayFlowTests {

        @Test
        @DisplayName("CAS success — resets orchestration fields and publishes step")
        void casSuccess_resetsAndPublishes() {
            TestFlow flow = newFlow("rf-1", FlowStatus.FAILED, "STEP_B");
            flow.setCorrelationId("corr-rf-1");
            flow.setRetryCount(5);
            flow.setErrorMessage("vendor timeout");
            flow.setBackoffSeconds(32);

            // CAS returns the flow with updated status
            TestFlow casResult = newFlow("rf-1", FlowStatus.IN_PROGRESS, "STEP_B");
            casResult.setCorrelationId("corr-rf-1");
            casResult.setRetryCount(0);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("rf-1"))
                    .thenReturn(Optional.of(flow))      // first call from replayFlow
                    .thenReturn(Optional.of(casResult)); // second call from casUpdateStatus

            TestFlow replayed = casOrchestrator.replayFlow("rf-1");

            assertNotNull(replayed);
            assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
            verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("CAS failure — fallback to inline save")
        void casFailure_fallbackInlineSave() {
            TestFlow flow = newFlow("rf-2", FlowStatus.FAILED, "STEP_A");
            flow.setCorrelationId("corr-rf-2");
            flow.setRetryCount(3);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("rf-2")).thenReturn(Optional.of(flow));

            TestFlow replayed = casOrchestrator.replayFlow("rf-2");

            assertNotNull(replayed);
            assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
            assertEquals(0, replayed.getRetryCount());
            assertNull(replayed.getErrorMessage());
            verify(flowRepo).save(flow);
        }

        @Test
        @DisplayName("CAS failure — status changed concurrently throws")
        void casFailure_statusChanged_throws() {
            TestFlow flow = newFlow("rf-3", FlowStatus.FAILED, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            // Re-read shows flow is now IN_PROGRESS (someone else replayed it)
            TestFlow changed = newFlow("rf-3", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("rf-3"))
                    .thenReturn(Optional.of(flow))   // first call from replayFlow
                    .thenReturn(Optional.of(changed)); // second call from CAS fallback

            assertThrows(IllegalStateException.class,
                    () -> casOrchestrator.replayFlow("rf-3"));
        }

        @Test
        @DisplayName("fromStep — resets completedSteps from that step onward")
        void fromStep_resetsCompletedSteps() {
            TestFlow flow = newFlow("rf-4", FlowStatus.FAILED, "STEP_C");
            flow.getCompletedSteps().addAll(Set.of("STEP_A", "STEP_B", "STEP_C"));

            when(flowRepo.findById("rf-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_B")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getStepsFromInclusive("STEP_B"))
                    .thenReturn(List.of("STEP_B", "STEP_C"));

            TestFlow replayed = orchestrator.replayFlow("rf-4", "STEP_B");

            assertEquals("STEP_B", replayed.getCurrentStep());
            assertTrue(replayed.getCompletedSteps().contains("STEP_A"));
            assertFalse(replayed.getCompletedSteps().contains("STEP_B"));
            assertFalse(replayed.getCompletedSteps().contains("STEP_C"));
        }

        @Test
        @DisplayName("fromStep — unknown step throws")
        void fromStep_unknownStep_throws() {
            TestFlow flow = newFlow("rf-5", FlowStatus.FAILED, "STEP_A");
            when(flowRepo.findById("rf-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("NONEXISTENT")).thenReturn(null);
            when(stepRegistry.getStepNames()).thenReturn(List.of("STEP_A", "STEP_B"));

            assertThrows(IllegalArgumentException.class,
                    () -> orchestrator.replayFlow("rf-5", "NONEXISTENT"));
        }

        @Test
        @DisplayName("allowCompleted=true — replays COMPLETED flow")
        void allowCompleted_replaysCompletedFlow() {
            TestFlow flow = newFlow("rf-6", FlowStatus.COMPLETED, "STEP_A");
            flow.setCorrelationId("corr-rf-6");
            when(flowRepo.findById("rf-6")).thenReturn(Optional.of(flow));

            TestFlow replayed = orchestrator.replayFlow("rf-6",
                    ReplayOptions.builder().allowCompleted(true).build());

            assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
        }

        @Test
        @DisplayName("allowCompleted=false — COMPLETED throws")
        void allowCompletedFalse_throws() {
            TestFlow flow = newFlow("rf-7", FlowStatus.COMPLETED, "STEP_A");
            when(flowRepo.findById("rf-7")).thenReturn(Optional.of(flow));

            assertThrows(IllegalStateException.class,
                    () -> orchestrator.replayFlow("rf-7"));
        }

        @Test
        @DisplayName("PARKED status — not replayable, throws")
        void parkedStatus_throws() {
            TestFlow flow = newFlow("rf-8", FlowStatus.PARKED, "STEP_A");
            when(flowRepo.findById("rf-8")).thenReturn(Optional.of(flow));

            assertThrows(IllegalStateException.class,
                    () -> orchestrator.replayFlow("rf-8"));
        }

        @Test
        @DisplayName("Flow not found — throws IllegalArgumentException")
        void flowNotFound_throws() {
            when(flowRepo.findById("rf-missing")).thenReturn(Optional.empty());

            assertThrows(IllegalArgumentException.class,
                    () -> orchestrator.replayFlow("rf-missing"));
        }

        @Test
        @DisplayName("COMPENSATION_FAILED — replayable")
        void compensationFailed_replayable() {
            TestFlow flow = newFlow("rf-9", FlowStatus.COMPENSATION_FAILED, "STEP_A");
            flow.setCorrelationId("corr-rf-9");
            when(flowRepo.findById("rf-9")).thenReturn(Optional.of(flow));

            TestFlow replayed = orchestrator.replayFlow("rf-9");

            assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
        }

        @Test
        @DisplayName("Kafka publish failure during replay does not throw")
        void kafkaFailure_noThrow() {
            TestFlow flow = newFlow("rf-10", FlowStatus.FAILED, "STEP_A");
            flow.setCorrelationId("corr-rf-10");
            when(flowRepo.findById("rf-10")).thenReturn(Optional.of(flow));
            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenThrow(new RuntimeException("Kafka down"));

            TestFlow replayed = orchestrator.replayFlow("rf-10");

            assertNotNull(replayed);
            assertEquals(FlowStatus.IN_PROGRESS, replayed.getStatus());
        }
    }

    // ====================================================================
    // markDeadLettered()
    // ====================================================================

    @Nested
    @DisplayName("markDeadLettered")
    class MarkDeadLetteredTests {

        @Test
        @DisplayName("CAS success — transitions to COMPENSATING and runs compensation")
        void casSuccess_compensates() {
            // CAS sets errorMessage via $set; findById after CAS returns flow with it
            TestFlow casResult = newFlow("dl-1", FlowStatus.COMPENSATING, "STEP_B");
            casResult.getCompletedSteps().add("STEP_A");
            casResult.setErrorMessage("[DLT] HTTP 500");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("dl-1")).thenReturn(Optional.of(casResult));
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));
            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            when(adapter.hasCompensation()).thenReturn(true);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapter);

            casOrchestrator.markDeadLettered("dl-1", "STEP_B", "HTTP 500");

            verify(adapter).compensate(casResult);
            assertTrue(casResult.getErrorMessage().contains("HTTP 500"));
        }

        @Test
        @DisplayName("Flow not found — logs and does not throw")
        void flowNotFound_noThrow() {
            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("dl-2")).thenReturn(Optional.empty());

            assertDoesNotThrow(() ->
                    casOrchestrator.markDeadLettered("dl-2", "STEP_A", "orphaned"));

            verify(stepLogRepo).save(any(StepExecutionLog.class));
        }

        @Test
        @DisplayName("Already COMPENSATING — skips duplicate compensation")
        void alreadyCompensating_skips() {
            TestFlow flow = newFlow("dl-3", FlowStatus.COMPENSATING, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("dl-3")).thenReturn(Optional.of(flow));

            casOrchestrator.markDeadLettered("dl-3", "STEP_A", "duplicate");

            // Should not save or compensate — already compensating (not in compensatable list)
            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("Already COMPLETED — skips")
        void alreadyCompleted_skips() {
            TestFlow flow = newFlow("dl-4", FlowStatus.COMPLETED, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("dl-4")).thenReturn(Optional.of(flow));

            casOrchestrator.markDeadLettered("dl-4", "STEP_A");

            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("CAS failure — fallback for inline mode (no mongoTemplate)")
        void fallback_inlineMode() {
            TestFlow flow = newFlow("dl-5", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("dl-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            orchestrator.markDeadLettered("dl-5", "STEP_A", "inline DLT");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
            assertTrue(flow.getErrorMessage().contains("inline DLT"));
        }

        @Test
        @DisplayName("No exceptionMessage — default message used")
        void noExceptionMessage_defaultMessage() {
            TestFlow flow = newFlow("dl-6", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("dl-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            orchestrator.markDeadLettered("dl-6");

            assertTrue(flow.getErrorMessage().contains("Exhausted all retry attempts"));
        }

        @Test
        @DisplayName("No stepName — uses flow currentStep in log")
        void noStepName_usesCurrentStep() {
            TestFlow flow = newFlow("dl-7", FlowStatus.WAITING_RETRY, "STEP_X");
            when(flowRepo.findById("dl-7")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_X")).thenReturn(List.of());

            orchestrator.markDeadLettered("dl-7", null, "error");

            ArgumentCaptor<StepExecutionLog> logCaptor = ArgumentCaptor.forClass(StepExecutionLog.class);
            verify(stepLogRepo, atLeastOnce()).save(logCaptor.capture());
            // At least one log entry should use the flow's currentStep
            boolean found = logCaptor.getAllValues().stream()
                    .anyMatch(l -> "STEP_X".equals(l.getStepName()));
            assertTrue(found, "Expected log entry with stepName=STEP_X");
        }
    }

    // ====================================================================
    // drainPendingSignals()
    // ====================================================================

    @Nested
    @DisplayName("drainPendingSignals")
    class DrainPendingSignalsTests {

        @Test
        @DisplayName("No signal registry — drain is no-op")
        void noSignalRegistry_noop() {
            TestFlow flow = newFlow("dr-1", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(flowRepo.findById("dr-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("dr-1", "STEP_A");

            assertEquals(FlowStatus.COMPLETED, flow.getStatus());
        }

        @Test
        @DisplayName("Drain with mongoTemplate reads from DB and syncs version")
        void mongoTemplate_readsFromDb() {
            TestFlow flow = newFlow("dr-2", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setVersion(1L);

            TestFlow dbSnapshot = newFlow("dr-2", FlowStatus.IN_PROGRESS, "STEP_A");
            dbSnapshot.setVersion(3L);
            var pending = new ArrayList<PendingSignal>();
            pending.add(new PendingSignal("approve", null, Instant.now()));
            dbSnapshot.setPendingSignals(pending);

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(mongoTemplate.findById("dr-2", TestFlow.class)).thenReturn(dbSnapshot);
            when(flowRepo.findById("dr-2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            casOrchestrator.executeStep("dr-2", "STEP_A");

            // Version should have been synced from DB snapshot
            assertEquals(3L, flow.getVersion());
            assertTrue(flow.isApproved());
        }

        @Test
        @DisplayName("Drain with unknown signal name — skips gracefully")
        void unknownSignal_skips() {
            TestFlow flow = newFlow("dr-3", FlowStatus.IN_PROGRESS, "STEP_A");
            var pending = new ArrayList<PendingSignal>();
            pending.add(new PendingSignal("unknown_signal", null, Instant.now()));
            flow.setPendingSignals(pending);

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            orchestrator.setSignalRegistry(sigRegistry);

            when(flowRepo.findById("dr-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            assertDoesNotThrow(() -> orchestrator.executeStep("dr-3", "STEP_A"));
            assertNull(flow.getPendingSignals());
        }

        @Test
        @DisplayName("Drain with signal handler exception — logs and continues")
        void signalHandlerException_continues() {
            TestFlow flow = newFlow("dr-4", FlowStatus.IN_PROGRESS, "STEP_A");
            var pending = new ArrayList<PendingSignal>();
            pending.add(new PendingSignal("approve", null, Instant.now()));
            flow.setPendingSignals(pending);

            SignalHandler<TestFlow> failingHandler = mock(SignalHandler.class);
            doThrow(new RuntimeException("handler error")).when(failingHandler).invoke(any(), any());
            when(failingHandler.getPayloadType()).thenReturn(null);

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve", failingHandler);
            orchestrator.setSignalRegistry(sigRegistry);

            when(flowRepo.findById("dr-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            assertDoesNotThrow(() -> orchestrator.executeStep("dr-4", "STEP_A"));
            assertNull(flow.getPendingSignals());
        }

        @Test
        @DisplayName("Empty pending signals — no processing")
        void emptyPendingSignals_noProcessing() {
            TestFlow flow = newFlow("dr-5", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setPendingSignals(new ArrayList<>());

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            when(flowRepo.findById("dr-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("dr-5", "STEP_A");

            assertFalse(flow.isApproved());
        }

        @Test
        @DisplayName("MongoTemplate findById fails — drain skipped gracefully")
        void mongoFindFails_drainSkipped() {
            TestFlow flow = newFlow("dr-6", FlowStatus.IN_PROGRESS, "STEP_A");

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(mongoTemplate.findById("dr-6", TestFlow.class))
                    .thenThrow(new RuntimeException("MongoDB read error"));
            when(flowRepo.findById("dr-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            assertDoesNotThrow(() -> casOrchestrator.executeStep("dr-6", "STEP_A"));
        }
    }

    // ====================================================================
    // saveFlowWithRetry()
    // ====================================================================

    @Nested
    @DisplayName("saveFlowWithRetry")
    class SaveFlowWithRetryTests {

        @Test
        @DisplayName("Version conflict — retries and succeeds on second attempt")
        void versionConflict_retriesAndSucceeds() {
            TestFlow flow = newFlow("sr-1", FlowStatus.IN_PROGRESS, "STEP_A");
            TestFlow freshFlow = newFlow("sr-1", FlowStatus.IN_PROGRESS, "STEP_A");
            freshFlow.setVersion(2L);

            when(flowRepo.findById("sr-1")).thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(freshFlow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            // First save throws version conflict, second succeeds
            when(flowRepo.save(any()))
                    .thenThrow(new OptimisticLockingFailureException("version conflict"))
                    .thenAnswer(inv -> inv.getArgument(0));

            orchestrator.executeStep("sr-1", "STEP_A");

            // At least 2 save attempts from saveFlowWithRetry (+ possible updateFlowPartial)
            verify(flowRepo, atLeast(2)).save(any());
        }

        @Test
        @DisplayName("Version conflict preserves new signals from concurrent push")
        void versionConflict_preservesSignals() {
            TestFlow flow = newFlow("sr-2", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setPendingSignals(null);

            TestFlow freshFlow = newFlow("sr-2", FlowStatus.IN_PROGRESS, "STEP_A");
            freshFlow.setVersion(2L);
            var signals = new ArrayList<PendingSignal>();
            signals.add(new PendingSignal("approve", null, Instant.now()));
            freshFlow.setPendingSignals(signals);

            when(flowRepo.findById("sr-2")).thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(freshFlow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            when(flowRepo.save(any()))
                    .thenThrow(new OptimisticLockingFailureException("conflict"))
                    .thenAnswer(inv -> inv.getArgument(0));

            orchestrator.executeStep("sr-2", "STEP_A");

            // Signals from fresh flow should be preserved
            assertNotNull(flow.getPendingSignals());
            assertEquals(1, flow.getPendingSignals().size());
        }

        @Test
        @DisplayName("All 3 retries fail — falls back to full partial update via mongoTemplate")
        void allRetriesFail_fullPartialUpdate() {
            TestFlow flow = newFlow("sr-3", FlowStatus.IN_PROGRESS, "STEP_A");
            TestFlow freshFlow = newFlow("sr-3", FlowStatus.IN_PROGRESS, "STEP_A");
            freshFlow.setVersion(5L);

            when(flowRepo.findById("sr-3")).thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(freshFlow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            // All saves fail
            when(flowRepo.save(any()))
                    .thenThrow(new OptimisticLockingFailureException("conflict"));

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));

            casOrchestrator.executeStep("sr-3", "STEP_A");

            // 3 save attempts
            verify(flowRepo, times(3)).save(any());
            // mongoTemplate used for claim + fallback partial update + advanceToNextStep
            verify(mongoTemplate, atLeast(1)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }
    }

    // ====================================================================
    // advanceToNextStep()
    // ====================================================================

    @Nested
    @DisplayName("advanceToNextStep")
    class AdvanceToNextStepTests {

        @Test
        @DisplayName("Last step — flow completed, no next step")
        void lastStep_flowCompleted() {
            TestFlow flow = newFlow("ad-1", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("ad-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("ad-1", "STEP_A");

            assertEquals(FlowStatus.COMPLETED, flow.getStatus());
        }

        @Test
        @DisplayName("CAS race detection — mod=0 skips duplicate send")
        void casRace_modZero_skipsDuplicate() {
            TestFlow flow = newFlow("ad-2", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1))  // claim
                    .thenReturn(ack(0)); // CAS for advance = someone else won
            when(flowRepo.findById("ad-2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
            when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));

            casOrchestrator.executeStep("ad-2", "STEP_A");

            // Kafka should NOT be called for advance — CAS failed (someone else won)
            verify(kafkaTemplate, never()).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("Parallel step publishing — multiple steps at same order")
        void parallelStepPublishing() {
            TestFlow flow = newFlow("ad-3", FlowStatus.IN_PROGRESS, "STEP_A");

            when(flowRepo.findById("ad-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B1");
            when(stepRegistry.getStepsAtSameOrder("STEP_B1"))
                    .thenReturn(List.of("STEP_B1", "STEP_B2", "STEP_B3"));

            orchestrator.executeStep("ad-3", "STEP_A");

            // Should publish all 3 parallel steps
            verify(kafkaTemplate, times(3)).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("CAS last step — mod=0 means already completed by another consumer")
        void casLastStep_alreadyCompleted() {
            TestFlow flow = newFlow("ad-4", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1))  // claim
                    .thenReturn(ack(0)); // CAS for complete = already done
            when(flowRepo.findById("ad-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            casOrchestrator.executeStep("ad-4", "STEP_A");

            // No Kafka publish for advance
            verify(kafkaTemplate, never()).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("Direct publish failure — outbox fallback created")
        void directPublishFailure_outboxFallback() {
            TestFlow flow = newFlow("ad-5", FlowStatus.IN_PROGRESS, "STEP_A");

            when(flowRepo.findById("ad-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
            when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));

            when(kafkaTemplate.send(eq("test.commands"), anyString(), anyString()))
                    .thenThrow(new RuntimeException("Kafka down"));

            orchestrator.executeStep("ad-5", "STEP_A");

            verify(outboxRepo).save(argThat(event ->
                    event != null && "test.commands".equals(event.getTopic())));
        }

        @Test
        @DisplayName("Direct publish and outbox both fail — logs error but does not throw")
        void publishAndOutboxBothFail_noThrow() {
            TestFlow flow = newFlow("ad-6", FlowStatus.IN_PROGRESS, "STEP_A");

            when(flowRepo.findById("ad-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
            when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));

            when(kafkaTemplate.send(eq("test.commands"), anyString(), anyString()))
                    .thenThrow(new RuntimeException("Kafka down"));
            when(outboxRepo.save(any())).thenThrow(new RuntimeException("MongoDB also down"));

            assertDoesNotThrow(() -> orchestrator.executeStep("ad-6", "STEP_A"));
        }
    }

    // ====================================================================
    // markParallelStepCompleted()
    // ====================================================================

    @Nested
    @DisplayName("markParallelStepCompleted")
    class MarkParallelStepCompletedTests {

        @Test
        @DisplayName("Parallel step — addToSet + re-read, not all done yet")
        void parallelStep_notAllDone() {
            TestFlow flow = newFlow("ps-1", FlowStatus.IN_PROGRESS, "STEP_A");

            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            when(adapter.isParallel()).thenReturn(true);
            when(adapter.getParallelGroup()).thenReturn("group1");
            when(adapter.getStepName()).thenReturn("STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));

            TestFlow freshFlow = newFlow("ps-1", FlowStatus.IN_PROGRESS, "STEP_A");
            freshFlow.getCompletedParallelSteps().add("STEP_A");

            when(flowRepo.findById("ps-1"))
                    .thenReturn(Optional.of(flow))  // doExecuteStepInner
                    .thenReturn(Optional.of(flow))  // after claim re-read
                    .thenReturn(Optional.of(freshFlow)); // after addToSet re-read

            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapter);

            MethodStepAdapter<TestFlow> siblingB = mock(MethodStepAdapter.class);
            when(siblingB.getStepName()).thenReturn("STEP_B");
            when(stepRegistry.getParallelGroup("group1"))
                    .thenReturn(List.of(adapter, siblingB));

            casOrchestrator.executeStep("ps-1", "STEP_A");

            // Should NOT advance — STEP_B not done
            verify(kafkaTemplate, never()).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("Parallel step — all done, advances to next step")
        void parallelStep_allDone_advances() {
            TestFlow flow = newFlow("ps-2", FlowStatus.IN_PROGRESS, "STEP_B");

            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            when(adapter.isParallel()).thenReturn(true);
            when(adapter.getParallelGroup()).thenReturn("group1");
            when(adapter.getStepName()).thenReturn("STEP_B");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));

            TestFlow freshFlow = newFlow("ps-2", FlowStatus.IN_PROGRESS, "STEP_B");
            freshFlow.getCompletedParallelSteps().addAll(Set.of("STEP_A", "STEP_B"));

            when(flowRepo.findById("ps-2"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(freshFlow));

            when(stepRegistry.getHandler("STEP_B")).thenReturn(adapter);

            MethodStepAdapter<TestFlow> siblingA = mock(MethodStepAdapter.class);
            when(siblingA.getStepName()).thenReturn("STEP_A");
            when(stepRegistry.getParallelGroup("group1"))
                    .thenReturn(List.of(siblingA, adapter));

            when(stepRegistry.getNextStep("STEP_B")).thenReturn("STEP_C");
            when(stepRegistry.getStepsAtSameOrder("STEP_C")).thenReturn(List.of("STEP_C"));

            casOrchestrator.executeStep("ps-2", "STEP_B");

            // Should advance — publish STEP_C
            verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("Inline mode — fallback sets + checks allDone")
        void inlineMode_fallback() {
            TestFlow flow = newFlow("ps-3", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.getCompletedParallelSteps().add("STEP_B");

            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            when(adapter.isParallel()).thenReturn(true);
            when(adapter.getParallelGroup()).thenReturn("group1");
            when(adapter.getStepName()).thenReturn("STEP_A");

            MethodStepAdapter<TestFlow> siblingB = mock(MethodStepAdapter.class);
            when(siblingB.getStepName()).thenReturn("STEP_B");

            when(flowRepo.findById("ps-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapter);
            when(stepRegistry.getParallelGroup("group1"))
                    .thenReturn(List.of(adapter, siblingB));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("ps-3", "STEP_A");

            assertTrue(flow.getCompletedParallelSteps().contains("STEP_A"));
            assertTrue(flow.getCompletedParallelSteps().contains("STEP_B"));
        }

        @Test
        @DisplayName("Sequential step — advances directly")
        void sequentialStep_advancesDirectly() {
            TestFlow flow = newFlow("ps-4", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(flowRepo.findById("ps-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
            when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));

            orchestrator.executeStep("ps-4", "STEP_A");

            verify(kafkaTemplate).send(eq("test.commands"), anyString(), anyString());
        }

        @Test
        @DisplayName("Parallel step re-read returns null — short-circuits")
        void parallelStep_reReadNull() {
            TestFlow flow = newFlow("ps-5", FlowStatus.IN_PROGRESS, "STEP_A");

            MethodStepAdapter<TestFlow> adapter = mock(MethodStepAdapter.class);
            when(adapter.isParallel()).thenReturn(true);
            when(adapter.getParallelGroup()).thenReturn("group1");
            when(adapter.getStepName()).thenReturn("STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));

            when(flowRepo.findById("ps-5"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.empty()); // re-read after addToSet returns null

            when(stepRegistry.getHandler("STEP_A")).thenReturn(adapter);

            casOrchestrator.executeStep("ps-5", "STEP_A");

            // Should not advance or throw
            verify(kafkaTemplate, never()).send(eq("test.commands"), anyString(), anyString());
        }
    }

    // ====================================================================
    // handleWaitingStep()
    // ====================================================================

    @Nested
    @DisplayName("handleWaitingStep")
    class HandleWaitingStepTests {

        @Test
        @DisplayName("PARKED mode with mongoTemplate — CAS sets waitingSince")
        void parked_mongoTemplate_setsWaitingSince() {
            TestFlow flow = newFlow("ws-1", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("waiting for approval",
                    WaitingStepException.WaitMode.PARKED, null, Duration.ofHours(48)))
                    .when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("ws-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            casOrchestrator.executeStep("ws-1", "STEP_A");

            // claim + updateFlowPartial + waitingSince CAS = at least 3 calls
            verify(mongoTemplate, atLeast(3)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("POLLING mode — sets nextRetryAt and increments pollCount")
        void polling_setsNextRetryAtAndPollCount() {
            TestFlow flow = newFlow("ws-2", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setPollCount(3);

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("polling signing status",
                    WaitingStepException.WaitMode.POLLING, Duration.ofSeconds(30), Duration.ofHours(72)))
                    .when(handler).execute(flow);

            when(flowRepo.findById("ws-2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            orchestrator.executeStep("ws-2", "STEP_A");

            assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
            assertNotNull(flow.getNextRetryAt());
            assertEquals(4, flow.getPollCount());
            assertNotNull(flow.getExpiresAt());
        }

        @Test
        @DisplayName("SLEEPING mode — PARKED status with nextRetryAt, no expiresAt")
        void sleeping_parkedWithNextRetryAt() {
            TestFlow flow = newFlow("ws-3", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("sleeping 1h",
                    WaitingStepException.WaitMode.SLEEPING, null, Duration.ofHours(1)))
                    .when(handler).execute(flow);

            when(flowRepo.findById("ws-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            orchestrator.executeStep("ws-3", "STEP_A");

            assertEquals(FlowStatus.PARKED, flow.getStatus());
            assertNotNull(flow.getNextRetryAt());
            assertNull(flow.getExpiresAt());
            assertNotNull(flow.getWaitingSince());
        }

        @Test
        @DisplayName("PARKED — does not reset waitingSince on re-entry")
        void parked_noResetWaitingSince() {
            Instant originalWaiting = Instant.now().minus(Duration.ofHours(1));
            TestFlow flow = newFlow("ws-4", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setWaitingSince(originalWaiting);
            flow.setExpiresAt(Instant.now().plus(Duration.ofHours(47)));

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("still waiting",
                    WaitingStepException.WaitMode.PARKED, null, Duration.ofHours(48)))
                    .when(handler).execute(flow);

            when(flowRepo.findById("ws-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            orchestrator.executeStep("ws-4", "STEP_A");

            assertEquals(originalWaiting, flow.getWaitingSince());
        }

        @Test
        @DisplayName("SLEEPING with mongoTemplate — partial update + waitingSince CAS")
        void sleeping_mongoTemplate_noExpiresAt() {
            TestFlow flow = newFlow("ws-5", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("sleeping 2h",
                    WaitingStepException.WaitMode.SLEEPING, null, Duration.ofHours(2)))
                    .when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("ws-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            casOrchestrator.executeStep("ws-5", "STEP_A");

            // claim + updateFlowPartial + waitingSince CAS
            verify(mongoTemplate, atLeast(3)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("POLLING with mongoTemplate — sets nextRetryAt in partial update")
        void polling_mongoTemplate_setsNextRetryAt() {
            TestFlow flow = newFlow("ws-6", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setPollCount(0);

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException("polling",
                    WaitingStepException.WaitMode.POLLING, Duration.ofSeconds(15), Duration.ofHours(1)))
                    .when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("ws-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            casOrchestrator.executeStep("ws-6", "STEP_A");

            // claim + updateFlowPartial + waitingSince CAS
            verify(mongoTemplate, atLeast(3)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("RETRYABLE with mongoTemplate — nextRetryAt stays null: retry topics own "
                + "redelivery, the scanner must not re-drive (would reset retry headers → unbounded)")
        void retryable_mongoTemplate_doesNotSetNextRetryAt() {
            TestFlow flow = newFlow("rt-own-1", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new RetryableStepException("vendor 503")).when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("rt-own-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            assertThrows(RetryableStepException.class,
                    () -> casOrchestrator.executeStep("rt-own-1", "STEP_A"));

            var updateCaptor = org.mockito.ArgumentCaptor.forClass(Update.class);
            verify(mongoTemplate, atLeast(1)).updateFirst(any(Query.class), updateCaptor.capture(), eq(TestFlow.class));
            var retryUpdate = updateCaptor.getAllValues().stream()
                    .map(u -> (org.bson.Document) u.getUpdateObject().get("$set"))
                    .filter(s -> s != null && s.containsKey("retryCount"))
                    .findFirst().orElseThrow(() -> new AssertionError("no retryable partial update captured"));
            assertEquals(FlowStatus.WAITING_RETRY.name(), retryUpdate.getString("status"));
            assertNull(retryUpdate.get("nextRetryAt"),
                    "nextRetryAt must be null(ed) — the Kafka retry-topic chain owns retryable "
                            + "redelivery; a set nextRetryAt makes the scanner inject fresh main-topic "
                            + "messages (unbounded) while the skip-guard starves the retry chain");
        }

        @Test
        @DisplayName("Null error message — uses default")
        void nullMessage_usesDefault() {
            TestFlow flow = newFlow("ws-7", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new WaitingStepException(null,
                    WaitingStepException.WaitMode.PARKED, null, Duration.ofHours(1)))
                    .when(handler).execute(flow);

            when(flowRepo.findById("ws-7")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            orchestrator.executeStep("ws-7", "STEP_A");

            assertEquals(FlowStatus.PARKED, flow.getStatus());
            assertEquals("waiting for external event", flow.getErrorMessage());
        }
    }

    // ====================================================================
    // signal()
    // ====================================================================

    @Nested
    @DisplayName("signal")
    class SignalTests {

        @Test
        @DisplayName("PARKED — immediate execute and re-publish step")
        void parked_immediateExecute() {
            TestFlow flow = newFlow("sg-1", FlowStatus.PARKED, "STEP_A");
            flow.setCorrelationId("corr-sg-1");
            when(flowRepo.findById("sg-1")).thenReturn(Optional.of(flow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            orchestrator.signal("sg-1", "approve", null);

            assertTrue(flow.isApproved());
            verify(flowRepo).save(flow);
            verify(kafkaTemplate).send(eq("test.commands"), eq("corr-sg-1"), anyString());
        }

        @Test
        @DisplayName("WAITING_RETRY — immediate execute and re-publish")
        void waitingRetry_immediateExecute() {
            TestFlow flow = newFlow("sg-2", FlowStatus.WAITING_RETRY, "STEP_A");
            when(flowRepo.findById("sg-2")).thenReturn(Optional.of(flow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            orchestrator.signal("sg-2", "approve", null);

            assertTrue(flow.isApproved());
            verify(flowRepo).save(flow);
        }

        @Test
        @DisplayName("IN_PROGRESS — queue as pendingSignal (inline mode)")
        void inProgress_queueInline() {
            TestFlow flow = newFlow("sg-3", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("sg-3")).thenReturn(Optional.of(flow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            orchestrator.signal("sg-3", "approve", "payload");

            assertFalse(flow.isApproved());
            assertNotNull(flow.getPendingSignals());
            assertEquals(1, flow.getPendingSignals().size());
            assertEquals("approve", flow.getPendingSignals().get(0).getSignalName());
            verify(flowRepo).save(flow);
        }

        @Test
        @DisplayName("IN_PROGRESS with mongoTemplate — atomic push succeeds")
        void inProgress_mongoTemplate_atomicPush() {
            TestFlow flow = newFlow("sg-4", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("sg-4")).thenReturn(Optional.of(flow));

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            casOrchestrator.signal("sg-4", "approve", null);

            assertFalse(flow.isApproved());
            verify(mongoTemplate).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("IN_PROGRESS with mongoTemplate — push fails, flow now PARKED, executes immediately")
        void inProgress_pushFails_nowParked_executesImmediately() {
            TestFlow flow = newFlow("sg-5", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setCorrelationId("corr-sg-5");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));

            TestFlow parkedFlow = newFlow("sg-5", FlowStatus.PARKED, "STEP_A");
            parkedFlow.setCorrelationId("corr-sg-5");
            when(flowRepo.findById("sg-5"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(parkedFlow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            casOrchestrator.signal("sg-5", "approve", null);

            assertTrue(parkedFlow.isApproved());
            verify(flowRepo).save(parkedFlow);
            verify(kafkaTemplate).send(eq("test.commands"), eq("corr-sg-5"), anyString());
        }

        @Test
        @DisplayName("IN_PROGRESS with mongoTemplate — push fails, flow now COMPLETED, signal dropped")
        void inProgress_pushFails_nowCompleted_signalDropped() {
            TestFlow flow = newFlow("sg-6", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));

            TestFlow completedFlow = newFlow("sg-6", FlowStatus.COMPLETED, "STEP_A");
            when(flowRepo.findById("sg-6"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(completedFlow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            casOrchestrator.signal("sg-6", "approve", null);

            assertFalse(completedFlow.isApproved());
            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("Non-actionable status — signal ignored")
        void nonActionableStatus_ignored() {
            TestFlow flow = newFlow("sg-7", FlowStatus.FAILED, "STEP_A");
            when(flowRepo.findById("sg-7")).thenReturn(Optional.of(flow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            orchestrator.signal("sg-7", "approve", null);

            assertFalse(flow.isApproved());
            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("No signal registry — throws")
        void noSignalRegistry_throws() {
            assertThrows(IllegalStateException.class,
                    () -> orchestrator.signal("sg-8", "approve", null));
        }

        @Test
        @DisplayName("Unknown signal — throws")
        void unknownSignal_throws() {
            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            orchestrator.setSignalRegistry(sigRegistry);

            assertThrows(IllegalArgumentException.class,
                    () -> orchestrator.signal("sg-9", "nonexistent", null));
        }

        @Test
        @DisplayName("Flow not found — throws")
        void flowNotFound_throws() {
            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            when(flowRepo.findById("sg-10")).thenReturn(Optional.empty());

            assertThrows(IllegalArgumentException.class,
                    () -> orchestrator.signal("sg-10", "approve", null));
        }

        @Test
        @DisplayName("PARKED — re-publish failure does not throw")
        void parked_republishFailure_noThrow() {
            TestFlow flow = newFlow("sg-11", FlowStatus.PARKED, "STEP_A");
            when(flowRepo.findById("sg-11")).thenReturn(Optional.of(flow));

            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenThrow(new RuntimeException("Kafka down"));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            assertDoesNotThrow(() -> orchestrator.signal("sg-11", "approve", null));
            assertTrue(flow.isApproved());
        }

        @Test
        @DisplayName("IN_PROGRESS inline — initializes pendingSignals list if null")
        void inProgress_initializesList() {
            TestFlow flow = newFlow("sg-12", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setPendingSignals(null);

            when(flowRepo.findById("sg-12")).thenReturn(Optional.of(flow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            orchestrator.setSignalRegistry(sigRegistry);

            orchestrator.signal("sg-12", "approve", null);

            assertNotNull(flow.getPendingSignals());
            assertEquals(1, flow.getPendingSignals().size());
        }

        @Test
        @DisplayName("IN_PROGRESS push fails, re-read returns null — signal dropped")
        void inProgress_pushFails_reReadNull_signalDropped() {
            TestFlow flow = newFlow("sg-13", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("sg-13"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.empty());

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            assertDoesNotThrow(() -> casOrchestrator.signal("sg-13", "approve", null));
        }

        @Test
        @DisplayName("IN_PROGRESS push fails, re-read WAITING_RETRY — executes immediately")
        void inProgress_pushFails_nowWaitingRetry_executes() {
            TestFlow flow = newFlow("sg-14", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));

            TestFlow waitingFlow = newFlow("sg-14", FlowStatus.WAITING_RETRY, "STEP_A");
            when(flowRepo.findById("sg-14"))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(waitingFlow));

            SignalRegistry<TestFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve",
                    new SignalHandler<>(new TestSignalHandlers(), getApproveMethod(), "approve"));
            casOrchestrator.setSignalRegistry(sigRegistry);

            casOrchestrator.signal("sg-14", "approve", null);

            assertTrue(waitingFlow.isApproved());
            verify(flowRepo).save(waitingFlow);
        }
    }

    // ====================================================================
    // Batch operations
    // ====================================================================

    @Nested
    @DisplayName("batchOperations")
    class BatchOperationTests {

        @Test
        @DisplayName("replayFlows — returns per-flow results")
        void replayFlows_perFlowResults() {
            TestFlow f1 = newFlow("batch-1", FlowStatus.FAILED, "STEP_A");
            f1.setCorrelationId("corr-b1");
            TestFlow f2 = newFlow("batch-2", FlowStatus.IN_PROGRESS, "STEP_A");

            when(flowRepo.findById("batch-1")).thenReturn(Optional.of(f1));
            when(flowRepo.findById("batch-2")).thenReturn(Optional.of(f2));

            var results = orchestrator.replayFlows(
                    List.of("batch-1", "batch-2"), ReplayOptions.builder().build());

            assertEquals(2, results.size());
            assertEquals("replayed", results.get(0).get("status"));
            assertEquals("error", results.get(1).get("status"));
        }

        @Test
        @DisplayName("cancelFlows — returns per-flow results")
        void cancelFlows_perFlowResults() {
            TestFlow f1 = newFlow("batch-3", FlowStatus.IN_PROGRESS, "STEP_A");
            TestFlow f2 = newFlow("batch-4", FlowStatus.COMPLETED, "STEP_A");

            when(flowRepo.findById("batch-3")).thenReturn(Optional.of(f1));
            when(flowRepo.findById("batch-4")).thenReturn(Optional.of(f2));
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            var results = orchestrator.cancelFlows(
                    List.of("batch-3", "batch-4"), "batch cancel");

            assertEquals(2, results.size());
            assertEquals("cancelled", results.get(0).get("status"));
            assertEquals("error", results.get(1).get("status"));
        }
    }

    // ====================================================================
    // executeStep — additional edge cases
    // ====================================================================

    @Nested
    @DisplayName("executeStep edge cases")
    class ExecuteStepEdgeCases {

        @Test
        @DisplayName("CANCELLED status — skips execution")
        void cancelledStatus_skips() {
            TestFlow flow = newFlow("ee-1", FlowStatus.CANCELLED, "STEP_A");
            when(flowRepo.findById("ee-1")).thenReturn(Optional.of(flow));

            orchestrator.executeStep("ee-1", "STEP_A");

            verify(stepRegistry, never()).getHandler(anyString());
        }

        @Test
        @DisplayName("CANCELLING status — skips execution")
        void cancellingStatus_skips() {
            TestFlow flow = newFlow("ee-2", FlowStatus.CANCELLING, "STEP_A");
            when(flowRepo.findById("ee-2")).thenReturn(Optional.of(flow));

            orchestrator.executeStep("ee-2", "STEP_A");

            verify(stepRegistry, never()).getHandler(anyString());
        }

        @Test
        @DisplayName("WAITING_RETRY with future nextRetryAt — skips")
        void waitingRetryFuture_skips() {
            TestFlow flow = newFlow("ee-3", FlowStatus.WAITING_RETRY, "STEP_A");
            flow.setNextRetryAt(Instant.now().plus(Duration.ofMinutes(5)));
            when(flowRepo.findById("ee-3")).thenReturn(Optional.of(flow));

            orchestrator.executeStep("ee-3", "STEP_A");

            verify(stepRegistry, never()).getHandler(anyString());
        }

        @Test
        @DisplayName("stepName null — uses flow.getCurrentStep()")
        void stepNameNull_usesCurrentStep() {
            TestFlow flow = newFlow("ee-4", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(flowRepo.findById("ee-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("ee-4", null);

            verify(stepRegistry).getHandler("STEP_A");
            verify(handler).execute(flow);
        }

        @Test
        @DisplayName("Atomic claim fails — skips execution (rebalance duplicate)")
        void atomicClaimFails_skips() {
            TestFlow flow = newFlow("ee-5", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(0));
            when(flowRepo.findById("ee-5")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));

            casOrchestrator.executeStep("ee-5", "STEP_A");

            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("Atomic claim succeeds — re-reads flow and executes")
        void atomicClaimSucceeds_executesStep() {
            TestFlow flow = newFlow("ee-6", FlowStatus.IN_PROGRESS, "STEP_A");

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("ee-6")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            casOrchestrator.executeStep("ee-6", "STEP_A");

            verify(mongoTemplate, atLeast(1)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("RetryableStepException propagated as-is")
        void retryableException_propagated() {
            TestFlow flow = newFlow("ee-7a", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("ee-7a")).thenReturn(Optional.of(flow));

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new RetryableStepException("explicit retry")).when(handler).execute(flow);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            var ex = assertThrows(RetryableStepException.class,
                    () -> orchestrator.executeStep("ee-7a", "STEP_A"));

            assertEquals("explicit retry", ex.getMessage());
        }

        @Test
        @DisplayName("NonRetryableStepException propagated as-is (no rethrow)")
        void nonRetryableException_notRethrown() {
            TestFlow flow = newFlow("ee-7b", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("ee-7b")).thenReturn(Optional.of(flow));

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("explicit fail")).when(handler).execute(flow);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            // NonRetryable does NOT rethrow — handled internally
            assertDoesNotThrow(() -> orchestrator.executeStep("ee-7b", "STEP_A"));
            assertEquals(FlowStatus.FAILED, flow.getStatus());
        }

        @Test
        @DisplayName("Generic RuntimeException wraps as RetryableStepException via infrastructure catch")
        void genericException_wrapsAsInfrastructure() {
            TestFlow flow = newFlow("ee-7c", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("ee-7c")).thenReturn(Optional.of(flow));

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            // Generic exception hits handleUnexpectedStepError -> StepErrorHandler.handleError
            // For a plain RuntimeException with no HTTP status and no annotations,
            // StepErrorHandler throws RetryableStepException("STEP_A failed: ...")
            doThrow(new RuntimeException("connection refused")).when(handler).execute(flow);
            when(handler.getStepName()).thenReturn("STEP_A");
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            var ex = assertThrows(RetryableStepException.class,
                    () -> orchestrator.executeStep("ee-7c", "STEP_A"));

            // The inner handleUnexpectedStepError wraps it, then executeStep outer catch
            // sees RetryableStepException and rethrows
            assertNotNull(ex.getMessage());
        }

        @Test
        @DisplayName("Parent flow context set on flow")
        void parentFlowContext_set() {
            TestFlow flow = newFlow("ee-8", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setParentFlowId("parent-1");
            flow.setParentFlowType("parent-type");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(flowRepo.findById("ee-8")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("ee-8", "STEP_A");

            verify(handler).execute(flow);
        }

        @Test
        @DisplayName("correlationId used as Kafka partition key")
        void correlationId_usedAsPartitionKey() {
            TestFlow flow = newFlow("ee-9", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setCorrelationId("my-correlation-key");

            when(flowRepo.findById("ee-9")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(mock(StepHandler.class));
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
            when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));

            orchestrator.executeStep("ee-9", "STEP_A");

            verify(kafkaTemplate).send(eq("test.commands"), eq("my-correlation-key"), anyString());
        }

        @Test
        @DisplayName("Step already in completedSteps but currentStep matches — falls through")
        void gateReactivation_fallsThrough() {
            TestFlow flow = newFlow("ee-10", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.getCompletedSteps().add("STEP_A"); // completed but currentStep is same

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(flowRepo.findById("ee-10")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.executeStep("ee-10", "STEP_A");

            // Handler IS executed — gate re-activation
            verify(handler).execute(flow);
        }
    }

    // ====================================================================
    // Compensation
    // ====================================================================

    @Nested
    @DisplayName("compensation")
    class CompensationTests {

        @Test
        @DisplayName("Compensation failure — sets COMPENSATION_FAILED status")
        void compensationFailure_setsCompensationFailed() {
            TestFlow flow = newFlow("co-1", FlowStatus.IN_PROGRESS, "STEP_B");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("bad request")).when(handler).execute(flow);

            MethodStepAdapter<TestFlow> stepA = mock(MethodStepAdapter.class);
            when(stepA.hasCompensation()).thenReturn(true);
            doThrow(new RuntimeException("compensation failed")).when(stepA).compensate(flow);

            when(flowRepo.findById("co-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_B")).thenReturn(handler);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepA);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.executeStep("co-1", "STEP_B");

            assertEquals(FlowStatus.COMPENSATION_FAILED, flow.getStatus());
            assertNotNull(flow.getCompensationError());
            assertTrue(flow.getCompensationError().contains("STEP_A"));
        }

        @Test
        @DisplayName("No completed steps — marks FAILED directly")
        void noCompletedSteps_failedDirectly() {
            TestFlow flow = newFlow("co-2", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("bad")).when(handler).execute(flow);

            when(flowRepo.findById("co-2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            orchestrator.executeStep("co-2", "STEP_A");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
        }

        @Test
        @DisplayName("Step without @Compensate — skipped")
        void noCompensate_skipped() {
            TestFlow flow = newFlow("co-3", FlowStatus.IN_PROGRESS, "STEP_B");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("bad")).when(handler).execute(flow);

            StepHandler<TestFlow> stepAHandler = mock(StepHandler.class);

            when(flowRepo.findById("co-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_B")).thenReturn(handler);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepAHandler);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.executeStep("co-3", "STEP_B");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
        }

        @Test
        @DisplayName("retryCompensation — re-runs compensation for COMPENSATION_FAILED")
        void retryCompensation_reRuns() {
            TestFlow flow = newFlow("co-4", FlowStatus.COMPENSATION_FAILED, "STEP_B");
            flow.setCompensationError("STEP_A: network error");

            MethodStepAdapter<TestFlow> stepA = mock(MethodStepAdapter.class);
            when(stepA.hasCompensation()).thenReturn(true);

            when(flowRepo.findById("co-4")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepA);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.retryCompensation("co-4");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
            assertNull(flow.getCompensationError());
            verify(stepA).compensate(flow);
        }

        @Test
        @DisplayName("retryCompensation — flow not found is no-op")
        void retryCompensation_notFound_noop() {
            when(flowRepo.findById("co-5")).thenReturn(Optional.empty());

            assertDoesNotThrow(() -> orchestrator.retryCompensation("co-5"));
        }

        @Test
        @DisplayName("retryCompensation — wrong status is no-op")
        void retryCompensation_wrongStatus_noop() {
            TestFlow flow = newFlow("co-6", FlowStatus.COMPLETED, "STEP_A");
            when(flowRepo.findById("co-6")).thenReturn(Optional.of(flow));

            orchestrator.retryCompensation("co-6");

            verify(stepRegistry, never()).getCompletedStepsBefore(anyString());
        }

        @Test
        @DisplayName("Successful compensation — sets FAILED status")
        void successfulCompensation_setsFailed() {
            TestFlow flow = newFlow("co-7", FlowStatus.IN_PROGRESS, "STEP_B");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("bad")).when(handler).execute(flow);

            MethodStepAdapter<TestFlow> stepA = mock(MethodStepAdapter.class);
            when(stepA.hasCompensation()).thenReturn(true);
            // Compensation succeeds (no exception)

            when(flowRepo.findById("co-7")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_B")).thenReturn(handler);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepA);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.executeStep("co-7", "STEP_B");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
            assertNull(flow.getCompensationError());
            verify(stepA).compensate(flow);
        }

        @Test
        @DisplayName("handlePermanentFailure with mongoTemplate — uses partial update")
        void permanentFailure_mongoTemplate_partialUpdate() {
            TestFlow flow = newFlow("co-8", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new NonRetryableStepException("bad")).when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("co-8")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());

            casOrchestrator.executeStep("co-8", "STEP_A");

            // claim + COMPENSATING partial update
            verify(mongoTemplate, atLeast(2)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }
    }

    // ====================================================================
    // advanceAfterReply
    // ====================================================================

    @Nested
    @DisplayName("advanceAfterReply")
    class AdvanceAfterReplyTests {

        @Test
        @DisplayName("Flow not found — throws")
        void flowNotFound_throws() {
            when(flowRepo.findById("aar-1")).thenReturn(Optional.empty());

            assertThrows(IllegalArgumentException.class,
                    () -> orchestrator.advanceAfterReply("aar-1", "STEP_A", null));
        }

        @Test
        @DisplayName("Terminal statuses — no-op")
        void terminalStatuses_noop() {
            for (FlowStatus status : List.of(FlowStatus.COMPLETED, FlowStatus.FAILED,
                    FlowStatus.CANCELLED, FlowStatus.CANCELLING)) {
                TestFlow flow = newFlow("aar-" + status.name(), status, "STEP_A");
                when(flowRepo.findById("aar-" + status.name())).thenReturn(Optional.of(flow));

                orchestrator.advanceAfterReply("aar-" + status.name(), "STEP_A", null);
            }
            // None should trigger step handler lookup
            verify(stepRegistry, never()).getHandler("STEP_A");
        }

        @Test
        @DisplayName("With valid flow snapshot — deserializes and uses it")
        void withFlowSnapshot_usesSnapshot() throws Exception {
            TestFlow flow = newFlow("aar-2", FlowStatus.IN_PROGRESS, "STEP_A");
            flow.setResult("some-result");
            String snapshot = objectMapper.writeValueAsString(flow);

            FlowOrchestrator<TestFlow> replyOrch = FlowOrchestrator.<TestFlow>builder()
                    .flowRepository(flowRepo)
                    .stepRegistry(stepRegistry)
                    .outboxRepository(outboxRepo)
                    .stepLogRepository(stepLogRepo)
                    .objectMapper(objectMapper)
                    .commandTopic("test.commands")
                    .replyTopic("test.replies")
                    .replyEnabled(true)
                    .kafkaTemplate(kafkaTemplate)
                    .build();
            replyOrch.setEntityClass(TestFlow.class);

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);
            // updateFlowPartial fallback calls findById
            when(flowRepo.findById("aar-2")).thenReturn(Optional.of(flow));

            replyOrch.advanceAfterReply("aar-2", "STEP_A", snapshot);

            // advanceAfterReply deserializes snapshot — the initial findById is skipped
            // (updateFlowPartial fallback may call findById, which is expected)
            verify(stepRegistry).getHandler("STEP_A");
        }

        @Test
        @DisplayName("Invalid snapshot — falls back to DB read")
        void invalidSnapshot_fallsToDb() {
            TestFlow flow = newFlow("aar-3", FlowStatus.IN_PROGRESS, "STEP_A");

            FlowOrchestrator<TestFlow> replyOrch = FlowOrchestrator.<TestFlow>builder()
                    .flowRepository(flowRepo)
                    .stepRegistry(stepRegistry)
                    .outboxRepository(outboxRepo)
                    .stepLogRepository(stepLogRepo)
                    .objectMapper(objectMapper)
                    .commandTopic("test.commands")
                    .replyTopic("test.replies")
                    .replyEnabled(true)
                    .kafkaTemplate(kafkaTemplate)
                    .build();
            replyOrch.setEntityClass(TestFlow.class);

            when(flowRepo.findById("aar-3")).thenReturn(Optional.of(flow));
            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            replyOrch.advanceAfterReply("aar-3", "STEP_A", "invalid json{{{");

            // Falls back to DB read
            verify(flowRepo, atLeastOnce()).findById("aar-3");
        }

        @Test
        @DisplayName("Empty snapshot — falls back to DB read")
        void emptySnapshot_fallsToDb() {
            TestFlow flow = newFlow("aar-4", FlowStatus.IN_PROGRESS, "STEP_A");

            when(flowRepo.findById("aar-4")).thenReturn(Optional.of(flow));
            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

            orchestrator.advanceAfterReply("aar-4", "STEP_A", "");

            verify(flowRepo, atLeastOnce()).findById("aar-4");
        }
    }

    // ====================================================================
    // startFlow
    // ====================================================================

    @Nested
    @DisplayName("startFlow")
    class StartFlowTests {

        @Test
        @DisplayName("startFlow — saves flow and writes outbox")
        void startFlow_savesAndWritesOutbox() {
            TestFlow flow = new TestFlow();
            flow.setCorrelationId("corr-sf-1");

            when(stepRegistry.getFirstStep()).thenReturn("STEP_A");
            when(flowRepo.save(any())).thenAnswer(inv -> {
                TestFlow f = inv.getArgument(0);
                f.setId("generated-id");
                return f;
            });

            TestFlow started = orchestrator.startFlow(flow);

            assertEquals("STEP_A", started.getCurrentStep());
            assertEquals(FlowStatus.IN_PROGRESS, started.getStatus());
            verify(flowRepo).save(flow);
            verify(outboxRepo).save(any(OutboxEvent.class));
        }

        @Test
        @DisplayName("Duplicate correlationId — returns existing flow")
        void duplicateCorrelationId_returnsExisting() {
            TestFlow existing = newFlow("existing-1", FlowStatus.IN_PROGRESS, "STEP_A");
            existing.setCorrelationId("corr-dup");

            TestFlow newFlow = new TestFlow();
            newFlow.setCorrelationId("corr-dup");

            when(mongoTemplate.findOne(any(Query.class), eq(TestFlow.class))).thenReturn(existing);

            TestFlow result = casOrchestrator.startFlow(newFlow);

            assertEquals("existing-1", result.getId());
            verify(flowRepo, never()).save(any());
        }

        @Test
        @DisplayName("Null correlationId — skips duplicate check")
        void nullCorrelationId_skipsDuplicateCheck() {
            TestFlow flow = new TestFlow();
            // No correlationId

            when(stepRegistry.getFirstStep()).thenReturn("STEP_A");

            casOrchestrator.startFlow(flow);

            verify(mongoTemplate, never()).findOne(any(Query.class), eq(TestFlow.class));
            verify(flowRepo).save(flow);
        }
    }

    // ====================================================================
    // findFlows
    // ====================================================================

    @Nested
    @DisplayName("findFlows")
    class FindFlowsTests {

        @Test
        @DisplayName("No mongoTemplate — returns empty")
        void noMongoTemplate_returnsEmpty() {
            var result = orchestrator.findFlows(Map.of("customerId", "c1"));
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Null attributes — returns empty")
        void nullAttributes_returnsEmpty() {
            var result = casOrchestrator.findFlows(null);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Empty attributes — returns empty")
        void emptyAttributes_returnsEmpty() {
            var result = casOrchestrator.findFlows(Map.of());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("With attributes — queries mongoTemplate")
        void withAttributes_queriesMongo() {
            TestFlow flow = newFlow("ff-1", FlowStatus.IN_PROGRESS, "STEP_A");
            when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                    .thenReturn(List.of(flow));

            var result = casOrchestrator.findFlows(Map.of("customerId", "c1"));

            assertEquals(1, result.size());
            verify(mongoTemplate).find(any(Query.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("Multiple attributes — AND query")
        void multipleAttributes_andQuery() {
            TestFlow flow = newFlow("ff-2", FlowStatus.COMPLETED, "DONE");
            when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                    .thenReturn(List.of(flow));

            var result = casOrchestrator.findFlows(
                    Map.of("status", "COMPLETED", "result", "success"));

            assertEquals(1, result.size());
            verify(mongoTemplate).find(any(Query.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("No matches — returns empty list")
        void noMatches_returnsEmpty() {
            when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                    .thenReturn(List.of());

            var result = casOrchestrator.findFlows(Map.of("result", "nonexistent"));

            assertTrue(result.isEmpty());
        }
    }

    // ====================================================================
    // Retryable failure with mongoTemplate
    // ====================================================================

    @Nested
    @DisplayName("handleRetryableFailure")
    class HandleRetryableFailureTests {

        @Test
        @DisplayName("With mongoTemplate — uses partial update")
        void mongoTemplate_partialUpdate() {
            TestFlow flow = newFlow("rf-m1", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new RetryableStepException("timeout")).when(handler).execute(flow);

            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class)))
                    .thenReturn(ack(1));
            when(flowRepo.findById("rf-m1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            assertThrows(RetryableStepException.class,
                    () -> casOrchestrator.executeStep("rf-m1", "STEP_A"));

            // claim + partial update for retryable failure
            verify(mongoTemplate, atLeast(2)).updateFirst(any(Query.class), any(Update.class), eq(TestFlow.class));
        }

        @Test
        @DisplayName("Without mongoTemplate — inline save with fields set")
        void inlineMode_fieldsSet() {
            TestFlow flow = newFlow("rf-m2", FlowStatus.IN_PROGRESS, "STEP_A");

            StepHandler<TestFlow> handler = mock(StepHandler.class);
            doThrow(new RetryableStepException("timeout")).when(handler).execute(flow);

            when(flowRepo.findById("rf-m2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

            assertThrows(RetryableStepException.class,
                    () -> orchestrator.executeStep("rf-m2", "STEP_A"));

            assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
            assertEquals(1, flow.getRetryCount());
            assertTrue(flow.getBackoffSeconds() > 0);
            assertNotNull(flow.getNextRetryAt());
            assertNull(flow.getExecutingStep());
            assertNull(flow.getExecutingPod());
        }
    }

    // ====================================================================
    // retryCompensation — additional edge cases
    // ====================================================================

    @Nested
    @DisplayName("retryCompensation edge cases")
    class RetryCompensationEdgeCases {

        @Test
        @DisplayName("COMPENSATING status is accepted (not just COMPENSATION_FAILED)")
        void compensatingStatus_accepted() {
            TestFlow flow = newFlow("rc-1", FlowStatus.COMPENSATING, "STEP_B");
            flow.setCompensationError("partial failure");

            MethodStepAdapter<TestFlow> stepA = mock(MethodStepAdapter.class);
            when(stepA.hasCompensation()).thenReturn(true);

            when(flowRepo.findById("rc-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepA);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.retryCompensation("rc-1");

            assertEquals(FlowStatus.FAILED, flow.getStatus());
            verify(stepA).compensate(flow);
        }

        @Test
        @DisplayName("compensationError is cleared before re-run")
        void compensationError_cleared() {
            TestFlow flow = newFlow("rc-2", FlowStatus.COMPENSATION_FAILED, "STEP_A");
            flow.setCompensationError("previous error");

            when(flowRepo.findById("rc-2")).thenReturn(Optional.of(flow));
            when(stepRegistry.getCompletedStepsBefore("STEP_A"))
                    .thenReturn(new ArrayList<>());

            orchestrator.retryCompensation("rc-2");

            assertNull(flow.getCompensationError());
        }

        @Test
        @DisplayName("compensation re-failure stays COMPENSATION_FAILED")
        void reFailure_staysCompensationFailed() {
            TestFlow flow = newFlow("rc-3", FlowStatus.COMPENSATION_FAILED, "STEP_B");

            MethodStepAdapter<TestFlow> stepA = mock(MethodStepAdapter.class);
            when(stepA.hasCompensation()).thenReturn(true);
            doThrow(new RuntimeException("still broken")).when(stepA).compensate(flow);

            when(flowRepo.findById("rc-3")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(stepA);
            when(stepRegistry.getCompletedStepsBefore("STEP_B"))
                    .thenReturn(new ArrayList<>(List.of("STEP_A")));

            orchestrator.retryCompensation("rc-3");

            assertEquals(FlowStatus.COMPENSATION_FAILED, flow.getStatus());
            assertNotNull(flow.getCompensationError());
        }

        @Test
        @DisplayName("IN_PROGRESS status is rejected")
        void inProgressStatus_rejected() {
            TestFlow flow = newFlow("rc-4", FlowStatus.IN_PROGRESS, "STEP_A");
            when(flowRepo.findById("rc-4")).thenReturn(Optional.of(flow));

            orchestrator.retryCompensation("rc-4");

            verify(stepRegistry, never()).getCompletedStepsBefore(anyString());
        }
    }

    // ====================================================================
    // executeStepOnly
    // ====================================================================

    @Nested
    @DisplayName("executeStepOnly")
    class ExecuteStepOnlyTests {

        @Test
        @DisplayName("delegates to executeStep")
        void delegatesToExecuteStep() {
            TestFlow flow = newFlow("eso-1", FlowStatus.IN_PROGRESS, "STEP_A");
            StepHandler<TestFlow> handler = mock(StepHandler.class);
            when(handler.getStepName()).thenReturn("STEP_A");

            when(flowRepo.findById("eso-1")).thenReturn(Optional.of(flow));
            when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
            when(stepRegistry.isLastStep("STEP_A")).thenReturn(false);
            when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");

            orchestrator.executeStepOnly("eso-1", "STEP_A");

            verify(handler).execute(flow);
        }

        @Test
        @DisplayName("flow not found throws")
        void flowNotFound_throws() {
            when(flowRepo.findById("eso-2")).thenReturn(Optional.empty());

            assertThrows(Exception.class, () ->
                    orchestrator.executeStepOnly("eso-2", "STEP_A"));
        }
    }

    // ====================================================================
    // shutdown
    // ====================================================================

    @Test
    @DisplayName("shutdown — no error when no executor")
    void shutdown_noError() {
        assertDoesNotThrow(() -> orchestrator.shutdown());
    }
}
