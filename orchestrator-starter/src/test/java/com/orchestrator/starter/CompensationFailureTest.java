package com.orchestrator.starter;

import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.MethodStepAdapter;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests that compensation/cancellation exception propagation works correctly.
 * Verifies that COMPENSATION_FAILED status is set when a @Compensate handler throws.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class CompensationFailureTest {

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

    @Test
    void compensation_handlerThrows_setsCompensationFailedStatus() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        // Step B handler throws NonRetryableStepException → triggers compensation
        StepHandler<TestFlow> handlerB = mock(StepHandler.class);
        when(handlerB.getStepName()).thenReturn("STEP_B");

        doThrow(new NonRetryableStepException("bad request")).when(handlerB).execute(flow);

        // Step A compensation handler throws
        MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
        when(adapterA.getStepName()).thenReturn("STEP_A");
        when(adapterA.hasCompensation()).thenReturn(true);
        doThrow(new RuntimeException("Compensation failed for step STEP_A",
                new RuntimeException("vendor API down"))).when(adapterA).compensate(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_B")).thenReturn(handlerB);
        when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
        when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of("STEP_A"));

        orchestrator.executeStep("flow-1", "STEP_B");

        // Flow should be COMPENSATION_FAILED, not just FAILED
        assertEquals(FlowStatus.COMPENSATION_FAILED, flow.getStatus());
        assertNotNull(flow.getCompensationError());
        assertTrue(flow.getCompensationError().contains("STEP_A"));
    }

    @Test
    void compensation_handlerSucceeds_setsFailedStatus() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handlerB = mock(StepHandler.class);
        when(handlerB.getStepName()).thenReturn("STEP_B");

        doThrow(new NonRetryableStepException("bad request")).when(handlerB).execute(flow);

        // Step A compensation succeeds
        MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
        when(adapterA.getStepName()).thenReturn("STEP_A");
        when(adapterA.hasCompensation()).thenReturn(true);
        doNothing().when(adapterA).compensate(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_B")).thenReturn(handlerB);
        when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
        when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of("STEP_A"));

        orchestrator.executeStep("flow-1", "STEP_B");

        // Successful compensation → FAILED (not COMPENSATION_FAILED)
        assertEquals(FlowStatus.FAILED, flow.getStatus());
        assertNull(flow.getCompensationError());
    }

    @Test
    void compensation_multipleSteps_partialFailure_setsCompensationFailed() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_C");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handlerC = mock(StepHandler.class);
        when(handlerC.getStepName()).thenReturn("STEP_C");

        doThrow(new NonRetryableStepException("step C failed")).when(handlerC).execute(flow);

        // Step B compensation succeeds
        MethodStepAdapter<TestFlow> adapterB = mock(MethodStepAdapter.class);
        when(adapterB.getStepName()).thenReturn("STEP_B");
        when(adapterB.hasCompensation()).thenReturn(true);
        doNothing().when(adapterB).compensate(flow);

        // Step A compensation fails
        MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
        when(adapterA.getStepName()).thenReturn("STEP_A");
        when(adapterA.hasCompensation()).thenReturn(true);
        doThrow(new RuntimeException("Compensation failed for step STEP_A",
                new RuntimeException("vendor unreachable"))).when(adapterA).compensate(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_C")).thenReturn(handlerC);
        when(stepRegistry.getHandler("STEP_B")).thenReturn(adapterB);
        when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
        when(stepRegistry.getCompletedStepsBefore("STEP_C")).thenReturn(List.of("STEP_A", "STEP_B"));

        orchestrator.executeStep("flow-1", "STEP_C");

        // Partial compensation failure → COMPENSATION_FAILED
        assertEquals(FlowStatus.COMPENSATION_FAILED, flow.getStatus());
        assertTrue(flow.getCompensationError().contains("STEP_A"));
        // Step B compensation should have been called (compensation continues after failure)
        verify(adapterB).compensate(flow);
        verify(adapterA).compensate(flow);
    }

    @Test
    void retryCompensation_retriesAfterCompensationFailed() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.COMPENSATION_FAILED);
        flow.setCompensationError("STEP_A: vendor down");

        // This time compensation succeeds
        MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
        when(adapterA.getStepName()).thenReturn("STEP_A");
        when(adapterA.hasCompensation()).thenReturn(true);
        doNothing().when(adapterA).compensate(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
        when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of("STEP_A"));

        orchestrator.retryCompensation("flow-1");

        // Should now be FAILED (compensation succeeded on retry)
        assertEquals(FlowStatus.FAILED, flow.getStatus());
        assertNull(flow.getCompensationError());
        verify(adapterA).compensate(flow);
    }

    @Test
    void cancellation_handlerThrows_logsCancelFailed() {
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        // Step A cancel handler throws
        MethodStepAdapter<TestFlow> adapterA = mock(MethodStepAdapter.class);
        when(adapterA.getStepName()).thenReturn("STEP_A");
        doThrow(new RuntimeException("Cancellation failed for step STEP_A",
                new RuntimeException("vendor API down"))).when(adapterA).cancel(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(adapterA);
        when(stepRegistry.getCompletedStepsBefore("STEP_B")).thenReturn(List.of("STEP_A"));

        // Cancel should not throw — cancellation failures are caught and logged
        var cancelled = orchestrator.cancelFlow("flow-1", "test cancel");

        assertNotNull(cancelled);
        assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
    }
}
