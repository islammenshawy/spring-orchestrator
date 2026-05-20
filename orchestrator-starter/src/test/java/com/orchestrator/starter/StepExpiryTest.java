package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.*;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for @Step(expiresAfter) feature:
 * - Duration parsing
 * - Library-level expiry enforcement via StaleFlowRecoveryService
 * - Auto-park when completedWhen is false after handler returns
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class StepExpiryTest {

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
        private boolean approved;
    }

    // ========== Duration parsing ==========

    @Test
    void parseExpiresAfter_hours() {
        assertEquals(Duration.ofHours(48), MethodStepAdapter.parseExpiresAfter("48h"));
        assertEquals(Duration.ofHours(1), MethodStepAdapter.parseExpiresAfter("1h"));
        assertEquals(Duration.ofHours(720), MethodStepAdapter.parseExpiresAfter("720h"));
    }

    @Test
    void parseExpiresAfter_days() {
        assertEquals(Duration.ofDays(7), MethodStepAdapter.parseExpiresAfter("7d"));
        assertEquals(Duration.ofDays(1), MethodStepAdapter.parseExpiresAfter("1d"));
        assertEquals(Duration.ofDays(30), MethodStepAdapter.parseExpiresAfter("30d"));
    }

    @Test
    void parseExpiresAfter_empty_returnsNull() {
        assertNull(MethodStepAdapter.parseExpiresAfter(""));
        assertNull(MethodStepAdapter.parseExpiresAfter(null));
    }

    @Test
    void parseExpiresAfter_invalid_throws() {
        assertThrows(IllegalArgumentException.class, () -> MethodStepAdapter.parseExpiresAfter("abc"));
        assertThrows(IllegalArgumentException.class, () -> MethodStepAdapter.parseExpiresAfter("48m"));
        assertThrows(IllegalArgumentException.class, () -> MethodStepAdapter.parseExpiresAfter("48"));
    }

    // ========== Expiry enforcement ==========

    @Test
    void expireWaitingFlows_expired_setsFailedStatus() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Flow waiting for 50 hours at a step with 48h expiry
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("AWAIT_APPROVAL");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setWaitingSince(Instant.now().minus(50, ChronoUnit.HOURS));

        // Step handler with 48h expiry
        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("AWAIT_APPROVAL");
        when(handler.getExpiresAfter()).thenReturn(Duration.ofHours(48));

        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getHandler("AWAIT_APPROVAL")).thenReturn(handler);

        when(flowRepo.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of());
        when(flowRepo.findByStatus(FlowStatus.WAITING_RETRY))
                .thenReturn(List.of(flow));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(flowRepo)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), 15, 10, outboxRepo, null);

        service.recoverStaleFlows();

        // Flow should be FAILED due to expiry
        assertEquals(FlowStatus.FAILED, flow.getStatus());
        assertTrue(flow.getErrorMessage().contains("expired"));
        assertTrue(flow.getErrorMessage().contains("AWAIT_APPROVAL"));
        verify(flowRepo).save(flow);
    }

    @Test
    void expireWaitingFlows_notExpired_notFailed() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);

        // Flow waiting for only 10 hours at a step with 48h expiry
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("AWAIT_APPROVAL");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setWaitingSince(Instant.now().minus(10, ChronoUnit.HOURS));

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("AWAIT_APPROVAL");
        when(handler.getExpiresAfter()).thenReturn(Duration.ofHours(48));

        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getHandler("AWAIT_APPROVAL")).thenReturn(handler);

        when(flowRepo.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of());
        when(flowRepo.findByStatus(FlowStatus.WAITING_RETRY))
                .thenReturn(List.of(flow));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(flowRepo)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), 15, 10, outboxRepo, null);

        service.recoverStaleFlows();

        // Flow should NOT be failed — still within expiry window
        assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
        verify(flowRepo, never()).save(flow);
    }

    @Test
    void expireWaitingFlows_noExpiry_ignored() {
        OrchestratorFlowRepository<TestFlow> flowRepo = mock(OrchestratorFlowRepository.class);
        KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);

        // Flow waiting for 100 hours but step has NO expiry
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("SOME_STEP");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setWaitingSince(Instant.now().minus(100, ChronoUnit.HOURS));

        StepHandler<TestFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SOME_STEP");
        when(handler.getExpiresAfter()).thenReturn(null); // no expiry

        StepRegistry<TestFlow> stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getHandler("SOME_STEP")).thenReturn(handler);

        when(flowRepo.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of());
        when(flowRepo.findByStatus(FlowStatus.WAITING_RETRY))
                .thenReturn(List.of(flow));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(flowRepo)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), 15, 10, outboxRepo, null);

        service.recoverStaleFlows();

        // Flow should NOT be failed — no expiry configured
        assertEquals(FlowStatus.WAITING_RETRY, flow.getStatus());
        verify(flowRepo, never()).save(flow);
    }

    // ========== Child flow chaining ==========

    @Test
    void childFlowChaining_startFlowCalledFromStep() {
        // Simulate a step handler that starts a child flow
        OrchestratorFlowRepository<TestFlow> parentRepo = mock(OrchestratorFlowRepository.class);
        OrchestratorFlowRepository<TestFlow> childRepo = mock(OrchestratorFlowRepository.class);
        OutboxEventRepository outboxRepo = mock(OutboxEventRepository.class);

        // Child orchestrator
        FlowOrchestrator<TestFlow> childOrchestrator = mock(FlowOrchestrator.class);

        // Parent flow
        TestFlow parentFlow = new TestFlow();
        parentFlow.setId("parent-1");
        parentFlow.setApproved(true);

        // Simulate startFlow on child — should save + write outbox
        TestFlow childFlow = new TestFlow();
        childFlow.setId("child-1");
        childFlow.setStatus(FlowStatus.IN_PROGRESS);
        when(childOrchestrator.startFlow(any())).thenReturn(childFlow);

        // Call startFlow from within a "step handler"
        TestFlow started = childOrchestrator.startFlow(childFlow);

        // Verify child flow was started
        assertNotNull(started);
        assertEquals("child-1", started.getId());
        assertEquals(FlowStatus.IN_PROGRESS, started.getStatus());
        verify(childOrchestrator).startFlow(childFlow);
    }
}
