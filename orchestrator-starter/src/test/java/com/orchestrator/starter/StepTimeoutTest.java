package com.orchestrator.starter;

import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class StepTimeoutTest {

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
    }

    @Test
    void executeStep_timeout_throwsRetryable() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);
        StepExecutionLogRepository stepLogRepo = mock(StepExecutionLogRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // 1-second timeout
        FlowOrchestrator<TestFlow> orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper())
                .flowType("test")
                .commandTopic("test.commands")
                .replyTopic("test.commands.replies")
                .replyEnabled(true)
                .kafkaTemplate(kafkaTemplate)
                .stepTimeoutSeconds(1)
                .build();

        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("SLOW_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SLOW_STEP");
        when(handler.getTimeoutSeconds()).thenReturn(-1); // inherit flow/global timeout

        // Simulate a hanging step (sleeps 5s, timeout is 1s)
        doAnswer(inv -> { Thread.sleep(5000); return null; }).when(handler).execute(flow);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("SLOW_STEP")).thenReturn(handler);

        RetryableStepException ex = assertThrows(RetryableStepException.class, () ->
                orchestrator.executeStep("flow-1", "SLOW_STEP"));

        assertTrue(ex.getMessage().contains("timed out"));
        assertTrue(ex.getMessage().contains("1s"));
    }

    @Test
    void executeStep_stepLevelTimeoutOverride_winsOverDisabledGlobal() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);
        StepExecutionLogRepository stepLogRepo = mock(StepExecutionLogRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Global/flow timeout DISABLED (0) — only the step-level override applies
        FlowOrchestrator<TestFlow> orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo).stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo).stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper()).flowType("test")
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).kafkaTemplate(kafkaTemplate)
                .stepTimeoutSeconds(0)
                .build();

        TestFlow flow = new TestFlow();
        flow.setId("flow-2");
        flow.setCurrentStep("SLOW_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SLOW_STEP");
        when(handler.getTimeoutSeconds()).thenReturn(1); // @Step(timeoutSeconds = 1)
        doAnswer(inv -> { Thread.sleep(5000); return null; }).when(handler).execute(flow);
        when(flowRepo.findById("flow-2")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("SLOW_STEP")).thenReturn(handler);

        RetryableStepException ex = assertThrows(RetryableStepException.class, () ->
                orchestrator.executeStep("flow-2", "SLOW_STEP"));
        assertTrue(ex.getMessage().contains("timed out after 1s"));
    }

    @Test
    void executeStep_stepLevelTimeoutZero_disablesTimeoutForThatStep() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);
        StepExecutionLogRepository stepLogRepo = mock(StepExecutionLogRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Global timeout 1s, but the step opts out (long batch sweep): @Step(timeoutSeconds = 0)
        FlowOrchestrator<TestFlow> orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo).stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo).stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper()).flowType("test")
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).kafkaTemplate(kafkaTemplate)
                .stepTimeoutSeconds(1)
                .build();

        TestFlow flow = new TestFlow();
        flow.setId("flow-3");
        flow.setCurrentStep("SWEEP_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SWEEP_STEP");
        when(handler.getTimeoutSeconds()).thenReturn(0); // step opts out of the timeout
        doAnswer(inv -> { Thread.sleep(2000); flow.setResult("done"); return null; })
                .when(handler).execute(flow);
        when(flowRepo.findById("flow-3")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("SWEEP_STEP")).thenReturn(handler);

        assertDoesNotThrow(() -> orchestrator.executeStep("flow-3", "SWEEP_STEP"));
        assertEquals("done", flow.getResult());
    }

    @Test
    void executeStep_noTimeout_executesNormally() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);
        StepExecutionLogRepository stepLogRepo = mock(StepExecutionLogRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Timeout disabled (0)
        FlowOrchestrator<TestFlow> orchestrator = FlowOrchestrator.<TestFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper())
                .flowType("test")
                .commandTopic("test.commands")
                .replyTopic("test.commands.replies")
                .replyEnabled(true)
                .kafkaTemplate(kafkaTemplate)
                .stepTimeoutSeconds(0)
                .build();

        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("FAST_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("FAST_STEP");


        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("FAST_STEP")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "FAST_STEP");

        verify(handler).execute(flow);
        verify(flowRepo).save(flow);
    }
}
