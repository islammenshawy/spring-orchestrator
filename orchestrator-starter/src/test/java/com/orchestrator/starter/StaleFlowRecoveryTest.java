package com.orchestrator.starter;

import com.mongodb.client.result.UpdateResult;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class StaleFlowRecoveryTest {

    private KafkaTemplate kafkaTemplate;
    private MongoTemplate mongoTemplate;
    private OutboxEventRepository outboxRepo;
    private OrchestratorMetrics metrics;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "flow_a")
    static class FlowA extends AbstractFlow {}

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "flow_b")
    static class FlowB extends AbstractFlow {}

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        mongoTemplate = mock(MongoTemplate.class);
        outboxRepo = mock(OutboxEventRepository.class);
        metrics = OrchestratorMetrics.noop();
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepo.countByFlowIdAndPublishedFalse(anyString())).thenReturn(0L);

        // Default: orphan cleanup finds nothing
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
    }

    private StaleFlowRecoveryService createService(FlowTypeRegistry registry) {
        return new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), mongoTemplate,
                15, 10, 100, 5, outboxRepo, metrics);
    }

    private FlowTypeDescriptor buildDescriptor(String flowType, Class<?> entityClass,
                                                 String commandTopic, StepRegistry<?> stepRegistry) {
        return FlowTypeDescriptor.builder()
                .flowType(flowType).entityClass(entityClass)
                .commandTopic(commandTopic).replyTopic(commandTopic + ".replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
    }

    @Test
    void recoverStaleFlows_claimsBatchAndPublishesToKafka() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCorrelationId("corr-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);
        staleA.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        // updateMulti for claim returns 1 claimed
        when(mongoTemplate.updateMulti(argThat(q -> {
            String json = q.toString();
            return json.contains("IN_PROGRESS") && json.contains("claimedBy");
        }), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // find claimed batch returns the flow
        when(mongoTemplate.find(argThat(q -> q.toString().contains("claimedBy")), eq(FlowA.class)))
                .thenReturn(List.of(staleA));

        // updateFirst for release
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // Kafka message published
        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-1"), contains("STEP_1"));
        // Release claim + inc recoveryCount
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    @Test
    void recoverStaleFlows_skipsFlowsWithPendingOutbox() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);

        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("IN_PROGRESS")),
                any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of(staleA));
        // Has pending outbox events
        when(outboxRepo.countByFlowIdAndPublishedFalse("a-1")).thenReturn(2L);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // Not published — skipped due to pending outbox
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        // But claim was released
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    @Test
    void recoverStaleFlows_exceedsMaxRecoveryAttempts_marksFailed() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);
        staleA.setRecoveryCount(10); // At max

        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("IN_PROGRESS")),
                any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of(staleA));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // Not published — exceeded max recovery attempts
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        // Status set to FAILED via updateFirst
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(FlowA.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
    }

    @Test
    void recoverStaleFlows_multiFlow_recoversAllFlowTypes() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCorrelationId("corr-a");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);

        FlowB staleB = new FlowB();
        staleB.setId("b-1");
        staleB.setCorrelationId("corr-b");
        staleB.setCurrentStep("CHARGE");
        staleB.setStatus(FlowStatus.IN_PROGRESS);

        // Both types claim successfully
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowB.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of(staleA));
        when(mongoTemplate.find(any(Query.class), eq(FlowB.class)))
                .thenReturn(List.of(staleB));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null),
                buildDescriptor("payment", FlowB.class, "payment.commands", null)));

        createService(registry).recoverStaleFlows();

        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-a"), contains("STEP_1"));
        verify(kafkaTemplate).send(eq("payment.commands"), eq("corr-b"), contains("CHARGE"));
    }

    @Test
    void recoverStaleFlows_nothingToClaim_skipsProcessing() {
        // updateMulti returns 0 — nothing to claim
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // No find, no publish, no release
        verify(mongoTemplate, never()).find(any(Query.class), eq(FlowA.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void recoverStaleFlows_kafkaFailure_releasesClaim() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCorrelationId("corr-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);

        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("IN_PROGRESS")),
                any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of(staleA));
        // Kafka send fails
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka down")));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // Claim released after failure
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    @Test
    void expireWaitingFlows_expiresStaleGateStep() {
        FlowA waitingFlow = new FlowA();
        waitingFlow.setId("a-1");
        waitingFlow.setCurrentStep("AWAIT_APPROVAL");
        waitingFlow.setStatus(FlowStatus.WAITING_RETRY);
        waitingFlow.setWaitingSince(Instant.now().minus(96, ChronoUnit.HOURS)); // 96h > 48h limit

        // Mock step handler with 48h expiry
        StepHandler handler = mock(StepHandler.class);
        when(handler.getExpiresAfter()).thenReturn(Duration.ofHours(48));
        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getStepNames()).thenReturn(List.of("AWAIT_APPROVAL"));
        when(stepRegistry.getHandler("AWAIT_APPROVAL")).thenReturn(handler);

        // Claim returns 1 expired flow
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("WAITING_RETRY")),
                any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(argThat(q -> q.toString().contains("AWAIT_APPROVAL")), eq(FlowA.class)))
                .thenReturn(List.of(waitingFlow));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // Verify status set to FAILED with expiry message
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(FlowA.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
        assertThat(updateStr).contains("expired");
    }

    @Test
    void orphanedClaims_releasedAtStartOfScan() {
        // First updateMulti call = orphan cleanup, rest = claim attempts
        AtomicInteger callCount = new AtomicInteger();
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenAnswer(inv -> {
                    int n = callCount.incrementAndGet();
                    if (n == 1) {
                        // Orphan cleanup: released 3 orphaned claims
                        return UpdateResult.acknowledged(3, 3L, null);
                    }
                    // Subsequent calls (claim attempts): nothing to claim
                    return UpdateResult.acknowledged(0, 0L, null);
                });

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // updateMulti called at least twice: orphan cleanup + claim attempt
        verify(mongoTemplate, atLeast(2)).updateMulti(any(Query.class), any(Update.class), eq(FlowA.class));
    }
}
