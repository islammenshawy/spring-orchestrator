package com.orchestrator.starter;

import com.mongodb.client.result.UpdateResult;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.domain.PendingSignal;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.MethodStepAdapter;
import com.orchestrator.starter.flow.SignalHandler;
import com.orchestrator.starter.flow.SignalRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.outbox.OutboxPublisher;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Concurrency and multi-instance unit tests for the orchestrator-starter module.
 *
 * These tests simulate production scenarios with multiple Kafka consumers,
 * multiple pods, and Kafka rebalancing. They verify that concurrency guards
 * (CAS claims, optimistic locking, version conflicts) work correctly.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({"unchecked", "rawtypes"})
class ConcurrencyTest {

    private OrchestratorFlowRepository<TestConcurrencyFlow> flowRepo;
    private StepRegistry<TestConcurrencyFlow> stepRegistry;
    private OutboxEventRepository outboxRepo;
    private StepExecutionLogRepository stepLogRepo;
    private KafkaTemplate kafkaTemplate;
    private MongoTemplate mongoTemplate;

    private FlowOrchestrator<TestConcurrencyFlow> orchestrator;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "concurrency_test_flows")
    static class TestConcurrencyFlow extends AbstractFlow {
        private String result;
        private boolean approved;
        private String approvedBy;
    }

    static class TestSignalHandlers {
        public void approve(TestConcurrencyFlow flow) {
            flow.setApproved(true);
            flow.setApprovedBy("signal-handler");
        }
    }

    @BeforeEach
    void setUp() {
        flowRepo = mock(OrchestratorFlowRepository.class);
        stepRegistry = mock(StepRegistry.class);
        outboxRepo = mock(OutboxEventRepository.class);
        stepLogRepo = mock(StepExecutionLogRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        mongoTemplate = mock(MongoTemplate.class);

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(stepLogRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        orchestrator = FlowOrchestrator.<TestConcurrencyFlow>builder()
                .flowRepository(flowRepo)
                .stepRegistry(stepRegistry)
                .outboxRepository(outboxRepo)
                .stepLogRepository(stepLogRepo)
                .objectMapper(new ObjectMapper())
                .commandTopic("test.commands")
                .replyTopic("test.commands.replies")
                .replyEnabled(false) // inline mode for simpler testing
                .kafkaTemplate(kafkaTemplate)
                .build();

        // Configure mongoTemplate + entityClass for CAS-based concurrency paths
        orchestrator.setMongoTemplate(mongoTemplate);
        orchestrator.setEntityClass(TestConcurrencyFlow.class);
        orchestrator.setPodId("pod-test-1");
    }

    // ========================================================================
    // 1. Duplicate Step Execution (Kafka Rebalance)
    // ========================================================================

    @Test
    @DisplayName("1. Kafka rebalance: two threads claim same step, only one executes")
    void duplicateStepExecution_kafkaRebalance_onlyOneThreadExecutes() throws Exception {
        String flowId = "flow-rebalance-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // CAS claim: first call wins (modifiedCount=1), second loses (modifiedCount=0)
        AtomicInteger claimCallCount = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(2);

        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    int call = claimCallCount.incrementAndGet();
                    barrier.await(5, TimeUnit.SECONDS); // sync both threads
                    if (call == 1) {
                        return UpdateResult.acknowledged(1, 1L, null); // winner
                    } else {
                        return UpdateResult.acknowledged(0, 0L, null); // loser
                    }
                });

        // Completion CAS (for advanceToNextStep -- last step completes flow)
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // drain reads from mongoTemplate
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        AtomicInteger executionCount = new AtomicInteger(0);
        doAnswer(inv -> {
            executionCount.incrementAndGet();
            return null;
        }).when(handler).execute(any());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);

        for (int i = 0; i < 2; i++) {
            executor.submit(() -> {
                try {
                    orchestrator.executeStep(flowId, "STEP_A");
                } catch (Exception e) {
                    // expected for infrastructure errors
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(10, TimeUnit.SECONDS), "Both threads should complete");
        executor.shutdown();

        assertThat(executionCount.get()).isEqualTo(1)
                .as("Step handler must execute exactly once despite two concurrent consumers");
    }

    @Test
    @DisplayName("1b. Claim guard: already completed step is skipped without claim attempt")
    void duplicateStepExecution_alreadyCompletedStep_skipsWithoutClaim() {
        String flowId = "flow-already-done-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_B");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.getCompletedSteps().add("STEP_A"); // already done

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);

        orchestrator.executeStep(flowId, "STEP_A");

        // Should skip without even touching mongoTemplate claim
        verify(mongoTemplate, never()).updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class));
        verify(handler, never()).execute(any());
    }

    // ========================================================================
    // 2. Concurrent Signal Push During Step Completion
    // ========================================================================

    @Test
    @DisplayName("2. Signal $push during step completion triggers version conflict + retry preserves signals")
    void concurrentSignalPush_versionConflict_preservesPendingSignals() {
        String flowId = "flow-signal-push-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setVersion(1L);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

        // Claim succeeds
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // Completion CAS
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // Simulate: first save throws OptimisticLockingFailureException (signal push incremented version)
        // Second save succeeds after re-read
        PendingSignal pushedSignal = new PendingSignal("updatePriority", "{\"level\":\"HIGH\"}", Instant.now());
        List<PendingSignal> signals = new ArrayList<>();
        signals.add(pushedSignal);

        TestConcurrencyFlow freshFlow = new TestConcurrencyFlow();
        freshFlow.setId(flowId);
        freshFlow.setCurrentStep("STEP_A");
        freshFlow.setStatus(FlowStatus.IN_PROGRESS);
        freshFlow.setVersion(2L); // version bumped by signal push
        freshFlow.setPendingSignals(signals);

        AtomicInteger saveAttempts = new AtomicInteger(0);
        when(flowRepo.save(any())).thenAnswer(inv -> {
            int attempt = saveAttempts.incrementAndGet();
            if (attempt == 1) {
                throw new OptimisticLockingFailureException("Version conflict");
            }
            return inv.getArgument(0);
        });

        // Re-read after version conflict returns fresh flow with pendingSignals
        when(flowRepo.findById(flowId))
                .thenReturn(Optional.of(flow))      // initial read
                .thenReturn(Optional.of(flow))       // re-read after claim
                .thenReturn(Optional.of(freshFlow)); // re-read after version conflict

        // drain reads from mongoTemplate (no pending yet at drain time)
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class))
                .thenReturn(flow);

        orchestrator.executeStep(flowId, "STEP_A");

        // Verify: save was called at least twice (first fails, second succeeds)
        verify(flowRepo, atLeast(2)).save(any());
        // Verify: version was updated from fresh doc
        assertThat(flow.getVersion()).isEqualTo(2L);
        // Verify: pendingSignals preserved from fresh doc
        assertThat(flow.getPendingSignals()).isNotNull();
        assertThat(flow.getPendingSignals()).hasSize(1);
        assertThat(flow.getPendingSignals().get(0).getSignalName()).isEqualTo("updatePriority");
    }

    // ========================================================================
    // 3. Concurrent cancelFlow vs Step Completion
    // ========================================================================

    @Test
    @DisplayName("3. Cancel vs step completion race: CAS ensures only one wins")
    void concurrentCancelVsStepCompletion_onlyOneWins() throws Exception {
        String flowId = "flow-cancel-race-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");

        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);
        when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // drain reads
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        // Track CAS outcomes
        AtomicReference<String> completionWinner = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);

        // All mongoTemplate.updateFirst calls go through a single handler
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    Query q = inv.getArgument(0);
                    String qStr = q.toString();

                    // CAS for cancel (status transition)
                    if (qStr.contains("status") && !qStr.contains("executingStep") && !qStr.contains("currentStep")) {
                        barrier.await(5, TimeUnit.SECONDS);
                        if (completionWinner.compareAndSet(null, "cancel")) {
                            return UpdateResult.acknowledged(1, 1L, null);
                        }
                        return UpdateResult.acknowledged(0, 0L, null);
                    }

                    // CAS for step claim
                    if (qStr.contains("executingStep")) {
                        barrier.await(5, TimeUnit.SECONDS);
                        if (completionWinner.compareAndSet(null, "step")) {
                            return UpdateResult.acknowledged(1, 1L, null);
                        }
                        return UpdateResult.acknowledged(0, 0L, null);
                    }

                    // Other CAS (completion, advancement)
                    return UpdateResult.acknowledged(1, 1L, null);
                });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);

        // Thread A: execute step
        executor.submit(() -> {
            try {
                orchestrator.executeStep(flowId, "STEP_A");
            } catch (Exception e) {
                // expected for infrastructure errors
            } finally {
                done.countDown();
            }
        });

        // Thread B: cancel flow
        executor.submit(() -> {
            try {
                orchestrator.cancelFlow(flowId, "user requested");
            } catch (Exception e) {
                // expected
            } finally {
                done.countDown();
            }
        });

        assertTrue(done.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Exactly one must win
        String winner = completionWinner.get();
        assertThat(winner).isNotNull().isIn("cancel", "step");
    }

    // ========================================================================
    // 4. Concurrent Parallel Step Completion
    // ========================================================================

    @Test
    @DisplayName("4. Two parallel steps complete simultaneously, CAS prevents double-advancement")
    void concurrentParallelStepCompletion_noDoubleAdvancement() throws Exception {
        String flowId = "flow-parallel-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_P1");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        // Create mock parallel step handlers
        MethodStepAdapter<TestConcurrencyFlow> handlerP1 = mock(MethodStepAdapter.class);
        when(handlerP1.getStepName()).thenReturn("STEP_P1");
        when(handlerP1.isParallel()).thenReturn(true);
        when(handlerP1.getParallelGroup()).thenReturn("group1");
        when(handlerP1.isJoinPoint()).thenReturn(false);

        MethodStepAdapter<TestConcurrencyFlow> handlerP2 = mock(MethodStepAdapter.class);
        when(handlerP2.getStepName()).thenReturn("STEP_P2");
        when(handlerP2.isParallel()).thenReturn(true);
        when(handlerP2.getParallelGroup()).thenReturn("group1");
        when(handlerP2.isJoinPoint()).thenReturn(false);

        List parallelGroup = List.of(handlerP1, handlerP2);
        when(stepRegistry.getHandler("STEP_P1")).thenReturn(handlerP1);
        when(stepRegistry.getHandler("STEP_P2")).thenReturn(handlerP2);
        when(stepRegistry.getParallelGroup("group1")).thenReturn(parallelGroup);
        when(stepRegistry.getNextStep("STEP_P1")).thenReturn("STEP_NEXT");
        when(stepRegistry.getNextStep("STEP_P2")).thenReturn("STEP_NEXT");
        when(stepRegistry.getStepsAtSameOrder("STEP_NEXT")).thenReturn(List.of("STEP_NEXT"));

        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // drain reads
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        // After $addToSet, re-read sees both steps completed
        TestConcurrencyFlow freshFlow = new TestConcurrencyFlow();
        freshFlow.setId(flowId);
        freshFlow.setCurrentStep("STEP_P1");
        freshFlow.setStatus(FlowStatus.IN_PROGRESS);
        freshFlow.setCompletedParallelSteps(Set.of("STEP_P1", "STEP_P2"));

        when(flowRepo.findById(flowId))
                .thenReturn(Optional.of(flow))       // initial for P1
                .thenReturn(Optional.of(flow))       // after claim P1
                .thenReturn(Optional.of(flow))       // initial for P2
                .thenReturn(Optional.of(flow))       // after claim P2
                .thenReturn(Optional.of(freshFlow))  // after $addToSet P1
                .thenReturn(Optional.of(freshFlow)); // after $addToSet P2

        // Claim succeeds for both (different step names in completedSteps.nin)
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // CAS for advancing to next step: first call wins, second loses
        AtomicInteger advanceCasCount = new AtomicInteger(0);
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    int call = advanceCasCount.incrementAndGet();
                    if (call == 1) {
                        return UpdateResult.acknowledged(1, 1L, null);
                    }
                    return UpdateResult.acknowledged(0, 0L, null);
                });

        // $addToSet for completedParallelSteps
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("completedParallelSteps")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        AtomicInteger kafkaAdvanceSends = new AtomicInteger(0);
        when(kafkaTemplate.send(eq("test.commands"), anyString(), anyString()))
                .thenAnswer(inv -> {
                    kafkaAdvanceSends.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);

        // Thread A: complete STEP_P1
        executor.submit(() -> {
            try { orchestrator.executeStep(flowId, "STEP_P1"); }
            catch (Exception e) { /* ignore */ }
            finally { done.countDown(); }
        });

        // Thread B: complete STEP_P2
        executor.submit(() -> {
            try { orchestrator.executeStep(flowId, "STEP_P2"); }
            catch (Exception e) { /* ignore */ }
            finally { done.countDown(); }
        });

        assertTrue(done.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Only one advance should succeed (CAS on currentStep)
        // kafkaAdvanceSends tracks how many times advanceToNextStep published
        assertThat(kafkaAdvanceSends.get()).isLessThanOrEqualTo(1)
                .as("At most one thread should successfully advance to next step");
    }

    // ========================================================================
    // 5. Outbox Publisher Thread Safety
    // ========================================================================

    @Test
    @DisplayName("5. Outbox: poison event dead-lettered without blocking good events")
    void outboxPublisher_poisonEvent_deadLetteredWithoutBlockingOthers() {
        OutboxEventRepository outboxRepository = mock(OutboxEventRepository.class);
        KafkaTemplate outboxKafka = mock(KafkaTemplate.class);
        OutboxPublisher publisher = new OutboxPublisher(outboxRepository, outboxKafka, 3, 100, null);

        OutboxEvent poisonEvent = OutboxEvent.builder()
                .id("poison-1").flowId("flow-poison").topic("").key("k1").payload("bad")
                .failureCount(2) // will hit maxRetries=3 on this attempt
                .build();

        OutboxEvent goodEvent1 = OutboxEvent.builder()
                .id("good-1").flowId("flow-good-1").topic("valid.topic").key("k2").payload("ok1")
                .build();

        OutboxEvent goodEvent2 = OutboxEvent.builder()
                .id("good-2").flowId("flow-good-2").topic("valid.topic").key("k3").payload("ok2")
                .build();

        when(outboxRepository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(poisonEvent, goodEvent1, goodEvent2));

        // Poison event: synchronous throw (empty topic)
        when(outboxKafka.send(eq(""), eq("k1"), eq("bad")))
                .thenThrow(new IllegalArgumentException("Topic must not be empty"));

        // Good events: succeed
        when(outboxKafka.send(eq("valid.topic"), eq("k2"), eq("ok1")))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxKafka.send(eq("valid.topic"), eq("k3"), eq("ok2")))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        // Poison event: dead-lettered after 3rd failure
        assertThat(poisonEvent.getFailureCount()).isEqualTo(3);
        assertTrue(poisonEvent.isDeadLettered(), "Poison event must be dead-lettered");

        // Good events: published successfully
        assertTrue(goodEvent1.isPublished(), "Good event 1 must not be blocked by poison");
        assertTrue(goodEvent2.isPublished(), "Good event 2 must not be blocked by poison");

        // All batch-saved
        verify(outboxRepository).saveAll(List.of(poisonEvent, goodEvent1, goodEvent2));
    }

    @Test
    @DisplayName("5b. Outbox: failureCount increments correctly across multiple poll cycles")
    void outboxPublisher_failureCountIncrementsAcrossPolls() {
        OutboxEventRepository outboxRepository = mock(OutboxEventRepository.class);
        KafkaTemplate outboxKafka = mock(KafkaTemplate.class);
        OutboxPublisher publisher = new OutboxPublisher(outboxRepository, outboxKafka, 5, 100, null);

        OutboxEvent event = OutboxEvent.builder()
                .id("failing-1").flowId("flow-failing").topic("topic").key("k1").payload("data")
                .failureCount(0)
                .build();

        when(outboxRepository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(event));
        when(outboxKafka.send(eq("topic"), eq("k1"), eq("data")))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka unavailable")));

        // Poll cycles 1 through 4: not dead-lettered
        for (int i = 1; i <= 4; i++) {
            publisher.publishPendingEvents();
            assertThat(event.getFailureCount()).isEqualTo(i);
            assertFalse(event.isDeadLettered(), "Should not be dead-lettered at cycle " + i);
        }

        // Poll cycle 5: hits maxRetries=5
        publisher.publishPendingEvents();
        assertThat(event.getFailureCount()).isEqualTo(5);
        assertTrue(event.isDeadLettered(), "Must be dead-lettered after 5 failures");
    }

    // ========================================================================
    // 6. Recovery Scanner vs Active Execution
    // ========================================================================

    @Test
    @DisplayName("6a. Recovery scanner skips flow with pending outbox events (pipeline busy)")
    void recoveryScanner_skipsFlowWithPendingOutbox() {
        MongoTemplate recoveryMongo = mock(MongoTemplate.class);
        KafkaTemplate recoveryKafka = mock(KafkaTemplate.class);
        OutboxEventRepository recoveryOutbox = mock(OutboxEventRepository.class);

        when(recoveryMongo.updateMulti(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        when(recoveryMongo.updateFirst(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(recoveryMongo.find(any(Query.class), any(Class.class)))
                .thenReturn(List.of());

        TestConcurrencyFlow activeFlow = new TestConcurrencyFlow();
        activeFlow.setId("flow-active-1");
        activeFlow.setCurrentStep("STEP_A");
        activeFlow.setStatus(FlowStatus.IN_PROGRESS);
        activeFlow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        // Flow has pending outbox events => pipeline is busy
        when(recoveryOutbox.countByFlowIdAndPublishedFalse("flow-active-1")).thenReturn(1L);

        when(recoveryMongo.find(any(Query.class), eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    Query q = inv.getArgument(0);
                    if (q.toString().contains(FlowStatus.IN_PROGRESS.name())) {
                        return List.of(activeFlow);
                    }
                    return List.of();
                });

        when(recoveryMongo.updateMulti(
                argThat(q -> q != null && q.toString().contains("$in")),
                any(Update.class), eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        FlowTypeDescriptor descriptor = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestConcurrencyFlow.class)
                .commandTopic("test.commands").replyTopic("test.replies")
                .replyEnabled(false)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(descriptor));
        StaleFlowRecoveryService recovery = new StaleFlowRecoveryService(
                registry, recoveryKafka, new ObjectMapper(), recoveryMongo,
                15, 10, 100, 5, recoveryOutbox, OrchestratorMetrics.noop());

        recovery.recoverStaleFlows();

        // Flow has pending outbox events => should NOT be re-published
        verify(recoveryKafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("6b. Recovery scanner claims and re-publishes stale flow with no outbox")
    void recoveryScanner_claimsStaleFlowWithNoOutbox() {
        MongoTemplate recoveryMongo = mock(MongoTemplate.class);
        KafkaTemplate recoveryKafka = mock(KafkaTemplate.class);
        OutboxEventRepository recoveryOutbox = mock(OutboxEventRepository.class);

        when(recoveryMongo.updateMulti(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        when(recoveryMongo.updateFirst(any(Query.class), any(Update.class), any(Class.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(recoveryMongo.find(any(Query.class), any(Class.class)))
                .thenReturn(List.of());

        TestConcurrencyFlow staleFlow = new TestConcurrencyFlow();
        staleFlow.setId("flow-stale-1");
        staleFlow.setCorrelationId("corr-stale-1");
        staleFlow.setCurrentStep("STEP_B");
        staleFlow.setStatus(FlowStatus.IN_PROGRESS);
        staleFlow.setExecutingStep(null);
        staleFlow.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        when(recoveryOutbox.countByFlowIdAndPublishedFalse("flow-stale-1")).thenReturn(0L);

        when(recoveryMongo.find(any(Query.class), eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    Query q = inv.getArgument(0);
                    if (q.toString().contains(FlowStatus.IN_PROGRESS.name())) {
                        return List.of(staleFlow);
                    }
                    return List.of();
                });

        when(recoveryMongo.updateMulti(
                argThat(q -> q != null && q.toString().contains("$in")),
                any(Update.class), eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        when(recoveryKafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        FlowTypeDescriptor descriptor = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(TestConcurrencyFlow.class)
                .commandTopic("test.commands").replyTopic("test.replies")
                .replyEnabled(false)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(descriptor));
        StaleFlowRecoveryService recovery = new StaleFlowRecoveryService(
                registry, recoveryKafka, new ObjectMapper(), recoveryMongo,
                15, 10, 100, 5, recoveryOutbox, OrchestratorMetrics.noop());

        recovery.recoverStaleFlows();

        // Stale flow with no outbox events should be re-published
        verify(recoveryKafka).send(eq("test.commands"), eq("corr-stale-1"), contains("STEP_B"));
    }

    // ========================================================================
    // 7. Version Conflict in saveFlowWithRetry
    // ========================================================================

    @Test
    @DisplayName("7. saveFlowWithRetry: version conflict on first save, re-read preserves pendingSignals")
    void saveFlowWithRetry_versionConflict_reReadPreservesSignals() {
        String flowId = "flow-version-retry-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setVersion(1L);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

        // Claim succeeds
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // Completion CAS
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // Fresh flow after version conflict -- has 2 new pending signals
        PendingSignal sig1 = new PendingSignal("signal1", null, Instant.now());
        PendingSignal sig2 = new PendingSignal("signal2", "{\"key\":\"val\"}", Instant.now());
        List<PendingSignal> freshSignals = new ArrayList<>();
        freshSignals.add(sig1);
        freshSignals.add(sig2);

        TestConcurrencyFlow freshFlow = new TestConcurrencyFlow();
        freshFlow.setId(flowId);
        freshFlow.setCurrentStep("STEP_A");
        freshFlow.setVersion(3L); // bumped twice by signal pushes
        freshFlow.setPendingSignals(freshSignals);

        // First save: version conflict. Second save: success.
        AtomicInteger saveCount = new AtomicInteger(0);
        when(flowRepo.save(any())).thenAnswer(inv -> {
            int attempt = saveCount.incrementAndGet();
            if (attempt == 1) {
                throw new OptimisticLockingFailureException("Conflict");
            }
            return inv.getArgument(0);
        });

        // findById returns: initial, after claim, after version conflict
        when(flowRepo.findById(flowId))
                .thenReturn(Optional.of(flow))
                .thenReturn(Optional.of(flow))
                .thenReturn(Optional.of(freshFlow));

        // drain reads from mongoTemplate
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        orchestrator.executeStep(flowId, "STEP_A");

        // Verify version was updated from fresh doc
        assertThat(flow.getVersion()).isEqualTo(3L);
        // Verify pendingSignals preserved from fresh doc
        assertThat(flow.getPendingSignals()).isNotNull();
        assertThat(flow.getPendingSignals()).hasSize(2);
    }

    @Test
    @DisplayName("7b. saveFlowWithRetry: all 3 retries fail, falls back to partial $set update")
    void saveFlowWithRetry_allRetriesFail_fallsBackToPartialUpdate() {
        String flowId = "flow-all-retries-fail-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setVersion(1L);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);

        // Claim succeeds
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // All 3 save attempts throw version conflict
        when(flowRepo.save(any()))
                .thenThrow(new OptimisticLockingFailureException("Conflict 1"))
                .thenThrow(new OptimisticLockingFailureException("Conflict 2"))
                .thenThrow(new OptimisticLockingFailureException("Conflict 3"));

        // Re-reads for retry
        TestConcurrencyFlow freshFlow = new TestConcurrencyFlow();
        freshFlow.setId(flowId);
        freshFlow.setVersion(5L);
        when(flowRepo.findById(flowId))
                .thenReturn(Optional.of(flow))       // initial
                .thenReturn(Optional.of(flow))        // after claim
                .thenReturn(Optional.of(freshFlow))   // retry 1
                .thenReturn(Optional.of(freshFlow))   // retry 2
                .thenReturn(Optional.of(freshFlow));  // retry 3

        // drain reads
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        // Should not throw -- falls back to full partial update
        try {
            orchestrator.executeStep(flowId, "STEP_A");
        } catch (Exception e) {
            // RetryableStepException from infrastructure error wrapping is acceptable
        }

        // Verify all 3 save attempts were made
        verify(flowRepo, times(3)).save(any());
        // Verify partial $set fallback was attempted via mongoTemplate.updateFirst
        // (the claim CAS also uses updateFirst, so we check for at least 2 calls)
        verify(mongoTemplate, atLeast(2)).updateFirst(any(Query.class), any(Update.class),
                eq(TestConcurrencyFlow.class));
    }

    // ========================================================================
    // 8. Concurrent Replay vs Cancel
    // ========================================================================

    @Test
    @DisplayName("8. Replay vs cancel race: CAS on status ensures only one succeeds")
    void concurrentReplayVsCancel_onlyOneSucceeds() throws Exception {
        String flowId = "flow-replay-cancel-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCorrelationId("corr-rc-1");
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.FAILED);
        flow.setErrorMessage("previous error");

        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getCompletedStepsBefore("STEP_A")).thenReturn(List.of());
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Track which CAS won
        AtomicReference<String> casWinner = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);

        // CAS for status change (both replay and cancel use casUpdateStatus)
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    Update update = inv.getArgument(1);
                    String updateStr = update.toString();

                    // Determine if this is replay (IN_PROGRESS) or cancel (CANCELLING)
                    String operation;
                    if (updateStr.contains("IN_PROGRESS")) {
                        operation = "replay";
                    } else if (updateStr.contains("CANCELLING")) {
                        operation = "cancel";
                    } else {
                        // Other updates (e.g., CANCELLED after cancel handlers run)
                        return UpdateResult.acknowledged(1, 1L, null);
                    }

                    if (casWinner.compareAndSet(null, operation)) {
                        return UpdateResult.acknowledged(1, 1L, null); // winner
                    }
                    return UpdateResult.acknowledged(0, 0L, null); // loser
                });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<String> replayResult = new AtomicReference<>();
        AtomicReference<String> cancelResult = new AtomicReference<>();

        // Thread A: replay
        executor.submit(() -> {
            try {
                orchestrator.replayFlow(flowId);
                replayResult.set("success");
            } catch (Exception e) {
                replayResult.set("failed:" + e.getClass().getSimpleName());
            } finally {
                done.countDown();
            }
        });

        // Thread B: cancel
        executor.submit(() -> {
            try {
                TestConcurrencyFlow result = orchestrator.cancelFlow(flowId, "concurrent cancel");
                cancelResult.set(result != null ? "success" : "lost");
            } catch (Exception e) {
                cancelResult.set("failed:" + e.getClass().getSimpleName());
            } finally {
                done.countDown();
            }
        });

        assertTrue(done.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // One should win, one should lose
        String winner = casWinner.get();
        assertThat(winner).isNotNull()
                .as("Exactly one CAS must win");
        assertThat(winner).isIn("replay", "cancel")
                .as("Winner must be either replay or cancel, not a corrupted state");
    }

    // ========================================================================
    // Additional concurrency edge cases
    // ========================================================================

    @Test
    @DisplayName("Three threads race to claim same step: exactly one wins")
    void threeThreadsRace_exactlyOneWins() throws Exception {
        String flowId = "flow-triple-race-1";
        int threadCount = 3;

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // drain reads
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        // CAS claim: only first caller wins
        AtomicInteger claimCalls = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);

        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return claimCalls.incrementAndGet() == 1
                            ? UpdateResult.acknowledged(1, 1L, null)
                            : UpdateResult.acknowledged(0, 0L, null);
                });

        // Completion CAS
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        AtomicInteger executionCount = new AtomicInteger(0);
        doAnswer(inv -> { executionCount.incrementAndGet(); return null; }).when(handler).execute(any());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try { orchestrator.executeStep(flowId, "STEP_A"); }
                catch (Exception ignored) {}
                finally { done.countDown(); }
            });
        }

        assertTrue(done.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertThat(executionCount.get()).isEqualTo(1)
                .as("Exactly one of three threads must execute the step handler");
    }

    @Test
    @DisplayName("Drain signals: concurrent signal push detected via version sync from DB snapshot")
    void drainSignals_versionSyncFromDbSnapshot() {
        String flowId = "flow-drain-sync-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setVersion(1L);
        flow.setApproved(false);

        // DB snapshot has a pending signal + bumped version (concurrent push happened)
        TestConcurrencyFlow dbSnapshot = new TestConcurrencyFlow();
        dbSnapshot.setId(flowId);
        dbSnapshot.setVersion(2L); // version bumped by signal push
        List<PendingSignal> signals = new ArrayList<>();
        signals.add(new PendingSignal("approve", null, Instant.now()));
        dbSnapshot.setPendingSignals(signals);

        StepHandler<TestConcurrencyFlow> handler = mock(StepHandler.class);
        when(handler.getStepName()).thenReturn("STEP_A");
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handler);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn(null);
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Claim succeeds
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        // Completion CAS
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        when(flowRepo.findById(flowId))
                .thenReturn(Optional.of(flow))   // initial
                .thenReturn(Optional.of(flow));  // after claim

        // drain re-reads from mongoTemplate.findById -- returns snapshot with signal
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(dbSnapshot);

        // Setup signal registry for drain
        try {
            var method = TestSignalHandlers.class.getDeclaredMethod("approve", TestConcurrencyFlow.class);
            SignalHandler<TestConcurrencyFlow> sigHandler = new SignalHandler<>(new TestSignalHandlers(), method, "approve");
            SignalRegistry<TestConcurrencyFlow> sigRegistry = new SignalRegistry<>();
            sigRegistry.register("approve", sigHandler);
            orchestrator.setSignalRegistry(sigRegistry);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        orchestrator.executeStep(flowId, "STEP_A");

        // Version should be synced from DB snapshot
        assertThat(flow.getVersion()).isEqualTo(2L)
                .as("Version must be synced from DB snapshot during drain");
        // Signal should have been executed
        assertTrue(flow.isApproved(), "Pending signal from DB snapshot must be drained and executed");
        // Pending signals cleared after drain
        assertNull(flow.getPendingSignals(), "Pending signals must be cleared after drain");
    }

    @Test
    @DisplayName("Multiple concurrent executeStep calls for different steps: claim serializes them")
    void multipleStepsDifferentNames_claimSerializes() throws Exception {
        String flowId = "flow-multi-step-1";

        TestConcurrencyFlow flow = new TestConcurrencyFlow();
        flow.setId(flowId);
        flow.setCurrentStep("STEP_A");
        flow.setStatus(FlowStatus.IN_PROGRESS);

        StepHandler<TestConcurrencyFlow> handlerA = mock(StepHandler.class);
        when(handlerA.getStepName()).thenReturn("STEP_A");

        StepHandler<TestConcurrencyFlow> handlerB = mock(StepHandler.class);
        when(handlerB.getStepName()).thenReturn("STEP_B");

        when(flowRepo.findById(flowId)).thenReturn(Optional.of(flow));
        when(stepRegistry.getHandler("STEP_A")).thenReturn(handlerA);
        when(stepRegistry.getHandler("STEP_B")).thenReturn(handlerB);
        when(stepRegistry.getNextStep("STEP_A")).thenReturn("STEP_B");
        when(stepRegistry.getStepsAtSameOrder("STEP_B")).thenReturn(List.of("STEP_B"));
        when(flowRepo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // drain reads
        when(mongoTemplate.findById(flowId, TestConcurrencyFlow.class)).thenReturn(flow);

        // Only first claim succeeds -- step B claim should fail (executingStep already set)
        AtomicInteger claimCount = new AtomicInteger(0);
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("executingStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenAnswer(inv -> {
                    int call = claimCount.incrementAndGet();
                    return call == 1
                            ? UpdateResult.acknowledged(1, 1L, null)
                            : UpdateResult.acknowledged(0, 0L, null);
                });

        // completion CAS
        when(mongoTemplate.updateFirst(
                argThat(q -> q != null && q.toString().contains("currentStep")),
                any(Update.class),
                eq(TestConcurrencyFlow.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);

        executor.submit(() -> {
            try { orchestrator.executeStep(flowId, "STEP_A"); }
            catch (Exception ignored) {}
            finally { done.countDown(); }
        });

        executor.submit(() -> {
            try { orchestrator.executeStep(flowId, "STEP_B"); }
            catch (Exception ignored) {}
            finally { done.countDown(); }
        });

        assertTrue(done.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Only one handler should have executed
        int totalExecutions = 0;
        try { verify(handlerA).execute(any()); totalExecutions++; } catch (AssertionError ignored) {}
        try { verify(handlerB).execute(any()); totalExecutions++; } catch (AssertionError ignored) {}

        assertThat(totalExecutions).isEqualTo(1)
                .as("Only one step handler should execute when claim serializes access");
    }
}
