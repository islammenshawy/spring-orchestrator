package com.orchestrator.starter;

import com.mongodb.client.result.UpdateResult;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.MethodStepAdapter;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class StaleFlowRecoveryTest {

    private KafkaTemplate kafkaTemplate;
    private MongoTemplate mongoTemplate;
    private OutboxEventRepository outboxRepo;

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
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepo.countByFlowIdAndPublishedFalse(anyString())).thenReturn(0L);

        // Default: orphan cleanup updates nothing, find returns empty
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(mongoTemplate.find(any(Query.class), any(Class.class)))
                .thenReturn(List.of());
    }

    private StaleFlowRecoveryService createService(FlowTypeRegistry registry) {
        return new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), mongoTemplate,
                15, 10, 100, 5, outboxRepo, OrchestratorMetrics.noop());
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

    /** Setup the two-step find-then-claim mock pattern for IN_PROGRESS recovery */
    @SuppressWarnings("unchecked")
    private void setupClaimPattern(Class<?> entityClass, List<?> candidates, long claimedCount) {
        // Return candidates only for IN_PROGRESS queries, empty for everything else
        when(mongoTemplate.find(any(Query.class), eq(entityClass)))
                .thenAnswer(inv -> {
                    Query q = inv.getArgument(0);
                    if (q.toString().contains(FlowStatus.IN_PROGRESS.name())) return candidates;
                    return List.of();
                });
        // updateMulti for claim returns claimedCount
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("$in")),
                any(Update.class), eq(entityClass)))
                .thenReturn(UpdateResult.acknowledged(claimedCount, claimedCount, null));
    }

    @Test
    void recoverStaleFlows_claimsBatchAndPublishesToKafka() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCorrelationId("corr-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);
        staleA.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        setupClaimPattern(FlowA.class, List.of(staleA), 1);

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-1"), contains("STEP_1"));
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    @Test
    void recoverStaleFlows_skipsFlowsWithPendingOutbox() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);

        setupClaimPattern(FlowA.class, List.of(staleA), 1);
        when(outboxRepo.countByFlowIdAndPublishedFalse("a-1")).thenReturn(2L);

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void recoverStaleFlows_exceedsMaxRecoveryAttempts_marksFailed() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);
        staleA.setRecoveryCount(10);

        setupClaimPattern(FlowA.class, List.of(staleA), 1);

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(FlowA.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        // Sets COMPENSATING first (crash-safe), then retryCompensation sets final status
        assertThat(updateStr).contains("COMPENSATING");
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

        // FlowA mocks — only return staleA for IN_PROGRESS queries
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenAnswer(inv -> {
                    if (inv.getArgument(0).toString().contains(FlowStatus.IN_PROGRESS.name())) return List.of(staleA);
                    return List.of();
                });
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // FlowB mocks
        when(mongoTemplate.find(any(Query.class), eq(FlowB.class)))
                .thenAnswer(inv -> {
                    if (inv.getArgument(0).toString().contains(FlowStatus.IN_PROGRESS.name())) return List.of(staleB);
                    return List.of();
                });
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowB.class)))
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
        // find candidates returns empty
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of());

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void recoverStaleFlows_kafkaFailure_releasesClaim() {
        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCorrelationId("corr-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);

        setupClaimPattern(FlowA.class, List.of(staleA), 1);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka down")));

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
        waitingFlow.setStatus(FlowStatus.PARKED);
        waitingFlow.setWaitingSince(Instant.now().minus(96, ChronoUnit.HOURS));
        waitingFlow.setExpiresAt(Instant.now().minus(48, ChronoUnit.HOURS)); // expired 48h ago

        StepRegistry stepRegistry = mock(StepRegistry.class);

        // find candidates returns the expired flow, find claimed returns same
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of(waitingFlow));
        when(mongoTemplate.updateMulti(argThat(q -> q.toString().contains("$in")),
                any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(FlowA.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("FAILED");
        assertThat(updateStr).contains("expired");
    }

    @Test
    void orphanedClaims_releasedAtStartOfScan() {
        AtomicInteger updateMultiCount = new AtomicInteger();
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenAnswer(inv -> {
                    int n = updateMultiCount.incrementAndGet();
                    if (n == 1) {
                        // Orphan cleanup: released 3
                        return UpdateResult.acknowledged(3, 3L, null);
                    }
                    return UpdateResult.acknowledged(0, 0L, null);
                });
        // find returns empty (no candidates after orphan cleanup)
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of());

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                buildDescriptor("enigio", FlowA.class, "enigio.commands", null)));

        createService(registry).recoverStaleFlows();

        // updateMulti called at least once for orphan cleanup
        verify(mongoTemplate, atLeast(1)).updateMulti(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    // ── recoverCompletedButNotAdvanced tests ──────────────────────────────

    /**
     * Helper: build a descriptor with a real StepRegistry mock that knows next steps.
     * Configures mongoTemplate.find to return the given flow for IN_PROGRESS queries
     * and empty for all others, WITHOUT any claim pattern (recoverCompletedButNotAdvanced
     * does its own unclaimed find, not the claim/batch pattern).
     */
    private FlowTypeDescriptor descriptorWithSteps(String flowType, Class<?> entityClass,
                                                    String commandTopic, StepRegistry<?> stepRegistry) {
        return FlowTypeDescriptor.builder()
                .flowType(flowType).entityClass(entityClass)
                .commandTopic(commandTopic).replyTopic(commandTopic + ".replies")
                .replyEnabled(true).repository(null)
                .stepRegistry(stepRegistry)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
    }

    /**
     * Configure find to return candidates for recoverCompletedButNotAdvanced
     * (IN_PROGRESS + stale + executingStep=null + claimedBy=null),
     * while returning empty for recoverFlowType's candidate query (which also
     * matches IN_PROGRESS) by counting invocations.
     */
    private void setupCompletedButNotAdvancedFind(Class<?> entityClass, List<?> candidates) {
        AtomicInteger findCallCount = new AtomicInteger();
        when(mongoTemplate.find(any(Query.class), eq(entityClass)))
                .thenAnswer(inv -> {
                    int call = findCallCount.incrementAndGet();
                    // Call 1 = recoverFlowType candidate query → empty (skip normal recovery)
                    // Call 2 = recoverCompletedButNotAdvanced candidate query → our candidates
                    if (call == 2) return candidates;
                    return List.of();
                });
    }

    @Test
    void recoverCompletedButNotAdvanced_advancesToNextStep() {
        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getNextStep("STEP_1")).thenReturn("STEP_2");

        FlowA flow = new FlowA();
        flow.setId("adv-1");
        flow.setCorrelationId("corr-adv");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("STEP_1");

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));
        // CAS update succeeds
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // Should publish STEP_2 (the next step), not STEP_1
        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-adv"), contains("STEP_2"));
    }

    @Test
    void recoverCompletedButNotAdvanced_skipsWhenCurrentStepNotInCompletedSteps() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("skip-1");
        flow.setCorrelationId("corr-skip");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        // completedSteps does NOT contain STEP_1

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // getNextStep should never be called — flow skipped
        verify(stepRegistry, never()).getNextStep(anyString());
        // No Kafka publish from recoverCompletedButNotAdvanced
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_2"));
    }

    @Test
    void recoverCompletedButNotAdvanced_skipsFlowWithFreshUpdatedAt() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("fresh-1");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(2, ChronoUnit.MINUTES)); // within threshold
        flow.getCompletedSteps().add("STEP_1");

        // Fresh flow should NOT appear in the MongoDB query results (filtered by updatedAt < threshold).
        // So mongo returns empty — this flow is never seen by the method.
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of());

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        verify(stepRegistry, never()).getNextStep(anyString());
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_2"));
    }

    @Test
    void recoverCompletedButNotAdvanced_skipsFlowWithExecutingStepSet() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("exec-1");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.setExecutingStep("STEP_1");
        flow.getCompletedSteps().add("STEP_1");

        // executingStep != null means MongoDB query filters it out (executingStep=null criteria)
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of());

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        verify(stepRegistry, never()).getNextStep(anyString());
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_1"));
    }

    @Test
    void recoverCompletedButNotAdvanced_lastStep_marksCompleted() {
        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getNextStep("STEP_FINAL")).thenReturn(null); // last step

        FlowA flow = new FlowA();
        flow.setId("last-1");
        flow.setCurrentStep("STEP_FINAL");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("STEP_FINAL");

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // Should mark COMPLETED, not publish to Kafka
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), updateCaptor.capture(), eq(FlowA.class));
        String updateStr = updateCaptor.getAllValues().stream()
                .map(Object::toString).reduce("", String::concat);
        assertThat(updateStr).contains("COMPLETED");
        // No step command should be published
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_FINAL"));
    }

    @Test
    void recoverCompletedButNotAdvanced_skipsFlowWithClaimedBySet() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("claimed-1");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.setClaimedBy("other-pod");
        flow.getCompletedSteps().add("STEP_1");

        // claimedBy != null means MongoDB query filters it out (claimedBy=null criteria)
        when(mongoTemplate.find(any(Query.class), eq(FlowA.class)))
                .thenReturn(List.of());

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        verify(stepRegistry, never()).getNextStep(anyString());
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void recoverCompletedButNotAdvanced_casFailure_skipsPublish() {
        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getNextStep("STEP_1")).thenReturn("STEP_2");

        FlowA flow = new FlowA();
        flow.setId("cas-fail-1");
        flow.setCorrelationId("corr-cas");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("STEP_1");

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));
        // CAS update returns modifiedCount=0 (another pod already advanced)
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 0L, null));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // Should NOT publish because CAS failed
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_2"));
    }

    @Test
    void recoverCompletedButNotAdvanced_kafkaFailure_logsButDoesNotThrow() {
        StepRegistry stepRegistry = mock(StepRegistry.class);
        when(stepRegistry.getNextStep("STEP_1")).thenReturn("STEP_2");

        FlowA flow = new FlowA();
        flow.setId("kafka-fail-1");
        flow.setCorrelationId("corr-kf");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("STEP_1");

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(FlowA.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka down")));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        // Should not throw
        createService(registry).recoverStaleFlows();

        // Kafka send was attempted
        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-kf"), contains("STEP_2"));
    }

    // ── recoverFlowType skip guard tests ──────────────────────────────────

    @Test
    void recoverFlowType_skipsFlowWhereCurrentStepInCompletedSteps() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("guard-1");
        flow.setCorrelationId("corr-guard");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("STEP_1"); // currentStep IS in completedSteps

        // All IN_PROGRESS find calls return this flow (both recoverFlowType and recoverCompletedButNotAdvanced)
        setupClaimPattern(FlowA.class, List.of(flow), 1);

        // For recoverCompletedButNotAdvanced: getNextStep returns STEP_2
        when(stepRegistry.getNextStep("STEP_1")).thenReturn("STEP_2");

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // recoverFlowType should NOT publish STEP_1 (skipped by guard)
        verify(kafkaTemplate, never()).send(anyString(), anyString(), contains("STEP_1"));
        // releaseClaim should be called (the guard releases claim before continuing)
        verify(mongoTemplate, atLeastOnce()).updateFirst(any(Query.class), any(Update.class), eq(FlowA.class));
    }

    @Test
    void recoverFlowType_normalRecovery_whenCurrentStepNotInCompletedSteps() {
        StepRegistry stepRegistry = mock(StepRegistry.class);

        FlowA flow = new FlowA();
        flow.setId("normal-1");
        flow.setCorrelationId("corr-normal");
        flow.setCurrentStep("STEP_1");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        // completedSteps does NOT contain STEP_1 → normal recovery path

        setupClaimPattern(FlowA.class, List.of(flow), 1);

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // recoverFlowType should re-publish STEP_1 (normal recovery)
        verify(kafkaTemplate).send(eq("enigio.commands"), eq("corr-normal"), contains("STEP_1"));
    }

    // ====================================================================
    // BUG 1: Recovery re-drives COMPLETED parallel sibling
    // ====================================================================

    @Test
    void parallelRecovery_shouldRepublishOnlyIncompleteSiblings() {
        // Setup: 4 parallel siblings (PULL_DNB, PULL_EQUIFAX, PULL_EXPERIAN, PULL_TRANSUNION)
        // currentStep pinned to PULL_DNB (first sibling)
        // PULL_DNB + PULL_EQUIFAX completed, PULL_EXPERIAN + PULL_TRANSUNION incomplete
        MethodStepAdapter<FlowA> pullDnb = mock(MethodStepAdapter.class);
        when(pullDnb.getStepName()).thenReturn("PULL_DNB");
        when(pullDnb.getOrder()).thenReturn(2);
        when(pullDnb.isParallel()).thenReturn(true);
        when(pullDnb.getParallelGroup()).thenReturn("pull");

        MethodStepAdapter<FlowA> pullEquifax = mock(MethodStepAdapter.class);
        when(pullEquifax.getStepName()).thenReturn("PULL_EQUIFAX");
        when(pullEquifax.getOrder()).thenReturn(2);
        when(pullEquifax.isParallel()).thenReturn(true);
        when(pullEquifax.getParallelGroup()).thenReturn("pull");

        MethodStepAdapter<FlowA> pullExperian = mock(MethodStepAdapter.class);
        when(pullExperian.getStepName()).thenReturn("PULL_EXPERIAN");
        when(pullExperian.getOrder()).thenReturn(2);
        when(pullExperian.isParallel()).thenReturn(true);
        when(pullExperian.getParallelGroup()).thenReturn("pull");

        MethodStepAdapter<FlowA> pullTransunion = mock(MethodStepAdapter.class);
        when(pullTransunion.getStepName()).thenReturn("PULL_TRANSUNION");
        when(pullTransunion.getOrder()).thenReturn(2);
        when(pullTransunion.isParallel()).thenReturn(true);
        when(pullTransunion.getParallelGroup()).thenReturn("pull");

        StepHandler<FlowA> seed = mock(StepHandler.class);
        when(seed.getStepName()).thenReturn("SEED");
        when(seed.getOrder()).thenReturn(1);

        StepHandler<FlowA> merge = mock(StepHandler.class);
        when(merge.getStepName()).thenReturn("MERGE");
        when(merge.getOrder()).thenReturn(3);

        StepRegistry<FlowA> stepRegistry = new StepRegistry<>(
                List.of(seed, pullDnb, pullEquifax, pullExperian, pullTransunion, merge));

        // Flow: currentStep=PULL_DNB (pinned to first sibling)
        // completedSteps includes SEED + PULL_DNB (first sibling completed)
        // completedParallelSteps: PULL_DNB + PULL_EQUIFAX (2 of 4 done)
        FlowA flow = new FlowA();
        flow.setId("parallel-1");
        flow.setCorrelationId("corr-parallel");
        flow.setCurrentStep("PULL_DNB");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("SEED");
        flow.getCompletedSteps().add("PULL_DNB");
        flow.getCompletedParallelSteps().add("PULL_DNB");
        flow.getCompletedParallelSteps().add("PULL_EQUIFAX");

        setupClaimPattern(FlowA.class, List.of(flow), 1);

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // EXPECTED: re-publish ONLY the 2 incomplete siblings (PULL_EXPERIAN, PULL_TRANSUNION)
        // NOT PULL_DNB (already completed)
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeast(1)).send(eq("enigio.commands"), eq("corr-parallel"), payloadCaptor.capture());

        List<String> publishedPayloads = payloadCaptor.getAllValues();
        List<String> publishedSteps = publishedPayloads.stream()
                .filter(p -> p.contains("PULL_"))
                .toList();

        // Must NOT re-publish completed siblings
        assertThat(publishedSteps).noneMatch(p -> p.contains("PULL_DNB"));
        assertThat(publishedSteps).noneMatch(p -> p.contains("PULL_EQUIFAX"));

        // Must re-publish incomplete siblings
        assertThat(publishedSteps).anyMatch(p -> p.contains("PULL_EXPERIAN"));
        assertThat(publishedSteps).anyMatch(p -> p.contains("PULL_TRANSUNION"));
    }

    // ====================================================================
    // BUG 2: recoverCompletedButNotAdvanced publishes only 1 of N siblings
    // ====================================================================

    @Test
    void recoverCompletedButNotAdvanced_parallelNextStep_shouldPublishAllSiblings() {
        // Setup: flow completed SEED, currentStep=SEED (still), next step is a parallel group
        MethodStepAdapter<FlowA> pullDnb = mock(MethodStepAdapter.class);
        when(pullDnb.getStepName()).thenReturn("PULL_DNB");
        when(pullDnb.getOrder()).thenReturn(2);
        when(pullDnb.isParallel()).thenReturn(true);
        when(pullDnb.getParallelGroup()).thenReturn("pull");

        MethodStepAdapter<FlowA> pullEquifax = mock(MethodStepAdapter.class);
        when(pullEquifax.getStepName()).thenReturn("PULL_EQUIFAX");
        when(pullEquifax.getOrder()).thenReturn(2);
        when(pullEquifax.isParallel()).thenReturn(true);
        when(pullEquifax.getParallelGroup()).thenReturn("pull");

        MethodStepAdapter<FlowA> pullExperian = mock(MethodStepAdapter.class);
        when(pullExperian.getStepName()).thenReturn("PULL_EXPERIAN");
        when(pullExperian.getOrder()).thenReturn(2);
        when(pullExperian.isParallel()).thenReturn(true);
        when(pullExperian.getParallelGroup()).thenReturn("pull");

        StepHandler<FlowA> seed = mock(StepHandler.class);
        when(seed.getStepName()).thenReturn("SEED");
        when(seed.getOrder()).thenReturn(1);

        StepHandler<FlowA> merge = mock(StepHandler.class);
        when(merge.getStepName()).thenReturn("MERGE");
        when(merge.getOrder()).thenReturn(3);

        StepRegistry<FlowA> stepRegistry = new StepRegistry<>(
                List.of(seed, pullDnb, pullEquifax, pullExperian, merge));

        // Flow: SEED completed but never advanced (reply lost)
        FlowA flow = new FlowA();
        flow.setId("advance-parallel-1");
        flow.setCorrelationId("corr-advance");
        flow.setCurrentStep("SEED");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));
        flow.getCompletedSteps().add("SEED");

        setupCompletedButNotAdvancedFind(FlowA.class, List.of(flow));

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(
                descriptorWithSteps("enigio", FlowA.class, "enigio.commands", stepRegistry)));

        createService(registry).recoverStaleFlows();

        // EXPECTED: should publish ALL 3 parallel siblings (PULL_DNB, PULL_EQUIFAX, PULL_EXPERIAN)
        // not just PULL_DNB (the first one returned by getNextStep)
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeast(1)).send(eq("enigio.commands"), anyString(), payloadCaptor.capture());

        List<String> publishedPayloads = payloadCaptor.getAllValues();

        assertThat(publishedPayloads).anyMatch(p -> p.contains("PULL_DNB"));
        assertThat(publishedPayloads).anyMatch(p -> p.contains("PULL_EQUIFAX"));
        assertThat(publishedPayloads).anyMatch(p -> p.contains("PULL_EXPERIAN"));
    }

    // ====================================================================
    // BUG 3: RetryableStepException cause swallowed
    // ====================================================================
    // (Tested in OrchestratorKafkaConsumer — separate test file)
}
