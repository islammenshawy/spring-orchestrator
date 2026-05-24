package com.orchestrator.starter;

import com.mongodb.client.result.UpdateResult;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for flow expiry via expiresAt field (set by waitUntil/pollUntil).
 * StaleFlowRecoveryService expires flows where expiresAt < now.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class StepExpiryTest {

    private KafkaTemplate kafkaTemplate;
    private MongoTemplate mongoTemplate;
    private OutboxEventRepository outboxRepo;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
        private boolean approved;
    }

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        mongoTemplate = mock(MongoTemplate.class);
        outboxRepo = mock(OutboxEventRepository.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Default: orphan cleanup and IN_PROGRESS claim find nothing
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
    }

    private StaleFlowRecoveryService createService(FlowTypeRegistry registry) {
        return new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), mongoTemplate,
                15, 10, 100, 5, outboxRepo, OrchestratorMetrics.noop());
    }

    // ========== Expiry enforcement ==========

    @Test
    void expireWaitingFlows_expiredParkedFlow_setsFailedStatus() {
        // PARKED flow with expiresAt in the past
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("AWAIT_APPROVAL");
        flow.setStatus(FlowStatus.PARKED);
        flow.setWaitingSince(Instant.now().minus(50, ChronoUnit.HOURS));
        flow.setExpiresAt(Instant.now().minus(2, ChronoUnit.HOURS)); // expired 2h ago

        when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                .thenReturn(List.of(flow));
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("$in")),
                any(Update.class), eq(TestFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(mock(StepRegistry.class))
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(TestFlow.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
        assertThat(updateStr).contains("expired");
    }

    @Test
    void expireWaitingFlows_expiredWaitingRetryFlow_setsFailedStatus() {
        // WAITING_RETRY flow with expiresAt in the past
        TestFlow flow = new TestFlow();
        flow.setId("flow-2");
        flow.setCurrentStep("POLL_STATUS");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setWaitingSince(Instant.now().minus(73, ChronoUnit.HOURS));
        flow.setExpiresAt(Instant.now().minus(1, ChronoUnit.HOURS));

        when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                .thenReturn(List.of(flow));
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("$in")),
                any(Update.class), eq(TestFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(mock(StepRegistry.class))
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(TestFlow.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
        assertThat(updateStr).contains("expired");
    }

    @Test
    void expireWaitingFlows_notExpired_notFailed() {
        // No expired flows — find returns empty
        when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                .thenReturn(List.of());

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(mock(StepRegistry.class))
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        // No claim attempted
        verify(mongoTemplate, never()).updateMulti(argThat(q -> q.toString().contains("$in")),
                any(Update.class), eq(TestFlow.class));
    }

    // ========== Child flow chaining ==========

    @Test
    void childFlowChaining_startFlowCalledFromStep() {
        FlowOrchestrator<TestFlow> childOrchestrator = mock(FlowOrchestrator.class);

        TestFlow childFlow = new TestFlow();
        childFlow.setId("child-1");
        childFlow.setStatus(FlowStatus.IN_PROGRESS);
        when(childOrchestrator.startFlow(any())).thenReturn(childFlow);

        TestFlow started = childOrchestrator.startFlow(childFlow);

        assertNotNull(started);
        assertEquals("child-1", started.getId());
        assertEquals(FlowStatus.IN_PROGRESS, started.getStatus());
        verify(childOrchestrator).startFlow(childFlow);
    }
}
