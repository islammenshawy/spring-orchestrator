package com.orchestrator.starter;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
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

        orchestrator = new FlowOrchestrator(
                flowRepo, stepRegistry, outboxRepo, stepLogRepo,
                new ObjectMapper(), "test.commands", "test.commands.replies", true,
                null, false, kafkaTemplate);
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
        verify(flowRepo).save(flow);
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
        verify(flowRepo).save(flow);
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

    // ========== Reply mode vs inline ==========

    @Test
    void executeStep_inlineMode_doesNotPublishReply() {
        FlowOrchestrator<TestFlow> inlineOrchestrator = new FlowOrchestrator(
                flowRepo, stepRegistry, outboxRepo, stepLogRepo,
                new ObjectMapper(), "test.commands", "", false, // reply disabled
                null, false, kafkaTemplate);

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
}
