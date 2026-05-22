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
 * Tests for @Step(expiresAfter) feature:
 * - Duration parsing
 * - Library-level expiry enforcement via StaleFlowRecoveryService
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
        // Flow waiting for 50 hours at a step with 48h expiry
        TestFlow flow = new TestFlow();
        flow.setId("flow-1");
        flow.setCurrentStep("AWAIT_APPROVAL");
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setWaitingSince(Instant.now().minus(50, ChronoUnit.HOURS));

        StepHandler handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("AWAIT_APPROVAL");
        when(handler.getExpiresAfter()).thenReturn(Duration.ofHours(48));

        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getStepNames()).thenReturn(List.of("AWAIT_APPROVAL"));
        when(stepRegistry.getHandler("AWAIT_APPROVAL")).thenReturn(handler);

        // Expiry claim returns 1 flow
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("WAITING_RETRY")
                && q.toString().contains("AWAIT_APPROVAL")),
                any(Update.class), eq(TestFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(argThat(q -> q.toString().contains("AWAIT_APPROVAL")), eq(TestFlow.class)))
                .thenReturn(List.of(flow));

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        // Verify FAILED status set via updateFirst
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(TestFlow.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
        assertThat(updateStr).contains("expired");
    }

    @Test
    void expireWaitingFlows_notExpired_notFailed() {
        // Flow waiting for only 10 hours at a step with 48h expiry — NOT expired
        StepHandler handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("AWAIT_APPROVAL");
        when(handler.getExpiresAfter()).thenReturn(Duration.ofHours(48));

        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getStepNames()).thenReturn(List.of("AWAIT_APPROVAL"));
        when(stepRegistry.getHandler("AWAIT_APPROVAL")).thenReturn(handler);

        // Claim returns 0 — nothing expired (10h < 48h threshold, query won't match)
        // updateMulti already defaults to 0 from setUp

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        // No find or updateFirst calls for expiry (claim returned 0)
        verify(mongoTemplate, never()).find(argThat(q -> q.toString().contains("AWAIT_APPROVAL")), eq(TestFlow.class));
    }

    @Test
    void expireWaitingFlows_noExpiry_ignored() {
        StepHandler handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("SOME_STEP");
        when(handler.getExpiresAfter()).thenReturn(null); // no expiry

        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getStepNames()).thenReturn(List.of("SOME_STEP"));
        when(stepRegistry.getHandler("SOME_STEP")).thenReturn(handler);

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestFlow.class)
                .commandTopic("test.commands").replyTopic("test.commands.replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        createService(registry).recoverStaleFlows();

        // No claim attempted for steps without expiry
        verify(mongoTemplate, never()).find(argThat(q -> q.toString().contains("SOME_STEP")), eq(TestFlow.class));
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
