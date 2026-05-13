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
        FlowOrchestrator<TestFlow> orchestrator = new FlowOrchestrator(
                flowRepo, stepRegistry, outboxRepo, stepLogRepo,
                new ObjectMapper(), "test", "test.commands", "test.commands.replies",
                true, null, false, kafkaTemplate, 1);

        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("SLOW_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SLOW_STEP");
        when(handler.isAlreadyCompleted(flow)).thenReturn(false);
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
        FlowOrchestrator<TestFlow> orchestrator = new FlowOrchestrator(
                flowRepo, stepRegistry, outboxRepo, stepLogRepo,
                new ObjectMapper(), "test", "test.commands", "test.commands.replies",
                true, null, false, kafkaTemplate, 0);

        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("FAST_STEP");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("FAST_STEP");
        when(handler.isAlreadyCompleted(flow)).thenReturn(false);

        when(flowRepo.findById("flow-1")).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("FAST_STEP")).thenReturn(handler);

        orchestrator.executeStep("flow-1", "FAST_STEP");

        verify(handler).execute(flow);
        verify(flowRepo).save(flow);
    }
}
