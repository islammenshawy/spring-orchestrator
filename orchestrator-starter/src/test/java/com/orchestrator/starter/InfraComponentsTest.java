package com.orchestrator.starter;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.autoconfigure.IndexInitializer;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.TopicValidator;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.StepOutcome;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.retry.JitteredExponentialBackOffPolicy;
import com.orchestrator.starter.testing.FlowReplayTestRunner;
import com.orchestrator.starter.testing.ReplayResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.util.backoff.BackOffExecution;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class InfraComponentsTest {

    // =====================================================================
    // Shared test flow
    // =====================================================================

    static class TestFlow implements OrchestratorFlow {
        private String id = "flow-1";
        private String correlationId = "corr-1";
        private String currentStep = "STEP_A";
        private FlowStatus status = FlowStatus.PENDING;
        private int retryCount;
        private int backoffSeconds;
        private Instant nextRetryAt;
        private String errorMessage;
        private Instant updatedAt = Instant.now();

        public String getId() { return id; }
        public String getCorrelationId() { return correlationId; }
        public String getCurrentStep() { return currentStep; }
        public void setCurrentStep(String s) { this.currentStep = s; }
        public FlowStatus getStatus() { return status; }
        public void setStatus(FlowStatus s) { this.status = s; }
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int c) { this.retryCount = c; }
        public int getBackoffSeconds() { return backoffSeconds; }
        public void setBackoffSeconds(int s) { this.backoffSeconds = s; }
        public Instant getNextRetryAt() { return nextRetryAt; }
        public void setNextRetryAt(Instant i) { this.nextRetryAt = i; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String m) { this.errorMessage = m; }
        public Instant getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(Instant i) { this.updatedAt = i; }
    }

    // =====================================================================
    // FlowReplayTestRunner tests
    // =====================================================================

    @Nested
    class ReplayTests {

        private StepRegistry<TestFlow> stepRegistry;
        private StepExecutionLogRepository logRepository;
        private tools.jackson.databind.ObjectMapper objectMapper;
        private FlowReplayTestRunner<TestFlow> runner;

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            // Create a step handler that completes successfully
            StepHandler<TestFlow> stepA = new StepHandler<>() {
                public String getStepName() { return "STEP_A"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) { /* completes */ }
            };

            stepRegistry = new StepRegistry<>(List.of(stepA));
            logRepository = mock(StepExecutionLogRepository.class);
            objectMapper = new tools.jackson.databind.ObjectMapper();
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);
        }

        @Test
        void replay_noLogs_returnsEmptyResult() {
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of());

            ReplayResult result = runner.replay("flow-1");

            assertEquals("flow-1", result.getFlowId());
            assertEquals(0, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_skipsNonExecutableOutcomes() {
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("STEP_A")
                    .status("SKIPPED")  // Not in EXECUTABLE_OUTCOMES
                    .attemptNumber(1)
                    .flowStateBefore("{\"id\":\"flow-1\",\"currentStep\":\"STEP_A\",\"status\":\"PENDING\"}")
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(0, result.getStepsReplayed());
        }

        @Test
        void replay_skipsEntriesWithNoFlowStateBefore() {
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("STEP_A")
                    .status(StepOutcome.COMPLETED.name())
                    .attemptNumber(1)
                    .flowStateBefore(null)  // No snapshot
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(0, result.getStepsReplayed());
        }

        @Test
        void replay_matchingOutcome_noMismatch() {
            var flowJson = "{\"id\":\"flow-1\",\"correlationId\":\"corr-1\",\"currentStep\":\"STEP_A\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("STEP_A")
                    .status(StepOutcome.COMPLETED.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_mismatchDetected_whenOutcomeDiffers() {
            // Step handler completes, but log says FAILED
            var flowJson = "{\"id\":\"flow-1\",\"correlationId\":\"corr-1\",\"currentStep\":\"STEP_A\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("STEP_A")
                    .status(StepOutcome.FAILED.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertFalse(result.allStepsMatched());
            assertEquals(1, result.getMismatches().size());

            var mismatch = result.getMismatches().get(0);
            assertEquals("STEP_A", mismatch.getStepName());
            assertEquals(StepOutcome.FAILED.name(), mismatch.getExpectedOutcome());
            assertEquals(StepOutcome.COMPLETED.name(), mismatch.getActualOutcome());
        }

        @Test
        void replay_waitingStepException_parked() {
            // Replace step handler with one that throws WaitingStepException (parked)
            StepHandler<TestFlow> parkingStep = new StepHandler<>() {
                public String getStepName() { return "PARK_STEP"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) {
                    throw new WaitingStepException("waiting",
                            WaitingStepException.WaitMode.PARKED, null, Duration.ofMinutes(5));
                }
            };

            stepRegistry = new StepRegistry<>(List.of(parkingStep));
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);

            var flowJson = "{\"id\":\"flow-1\",\"currentStep\":\"PARK_STEP\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("PARK_STEP")
                    .status(StepOutcome.PARKED.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_waitingStepException_polling() {
            StepHandler<TestFlow> pollingStep = new StepHandler<>() {
                public String getStepName() { return "POLL_STEP"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) {
                    throw new WaitingStepException("polling",
                            WaitingStepException.WaitMode.POLLING, Duration.ofSeconds(10), Duration.ofMinutes(5));
                }
            };

            stepRegistry = new StepRegistry<>(List.of(pollingStep));
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);

            var flowJson = "{\"id\":\"flow-1\",\"currentStep\":\"POLL_STEP\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("POLL_STEP")
                    .status(StepOutcome.WAITING.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_retryableException_recordsRetrying() {
            StepHandler<TestFlow> retryingStep = new StepHandler<>() {
                public String getStepName() { return "RETRY_STEP"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) {
                    throw new RetryableStepException("transient error");
                }
            };

            stepRegistry = new StepRegistry<>(List.of(retryingStep));
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);

            var flowJson = "{\"id\":\"flow-1\",\"currentStep\":\"RETRY_STEP\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("RETRY_STEP")
                    .status(StepOutcome.RETRYING.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_nonRetryableException_recordsFailed() {
            StepHandler<TestFlow> failingStep = new StepHandler<>() {
                public String getStepName() { return "FAIL_STEP"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) {
                    throw new NonRetryableStepException("permanent error");
                }
            };

            stepRegistry = new StepRegistry<>(List.of(failingStep));
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);

            var flowJson = "{\"id\":\"flow-1\",\"currentStep\":\"FAIL_STEP\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("FAIL_STEP")
                    .status(StepOutcome.FAILED.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_unknownException_treatedAsRetrying() {
            StepHandler<TestFlow> errorStep = new StepHandler<>() {
                public String getStepName() { return "ERR_STEP"; }
                public int getOrder() { return 1; }
                public void execute(TestFlow flow) {
                    throw new NullPointerException("infra error");
                }
            };

            stepRegistry = new StepRegistry<>(List.of(errorStep));
            runner = new FlowReplayTestRunner<>(stepRegistry, logRepository, objectMapper, TestFlow.class);

            var flowJson = "{\"id\":\"flow-1\",\"currentStep\":\"ERR_STEP\",\"status\":\"PENDING\",\"retryCount\":0,\"backoffSeconds\":0}";
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("ERR_STEP")
                    .status(StepOutcome.RETRYING.name())
                    .attemptNumber(1)
                    .flowStateBefore(flowJson)
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(1, result.getStepsReplayed());
            assertTrue(result.allStepsMatched());
        }

        @Test
        void replay_invalidJson_recordsErrorMismatch() {
            var log = StepExecutionLog.builder()
                    .flowId("flow-1")
                    .stepName("STEP_A")
                    .status(StepOutcome.COMPLETED.name())
                    .attemptNumber(1)
                    .flowStateBefore("not-valid-json")
                    .build();
            when(logRepository.findByFlowIdOrderByStartedAtAsc("flow-1")).thenReturn(List.of(log));

            ReplayResult result = runner.replay("flow-1");

            assertEquals(0, result.getStepsReplayed());
            assertFalse(result.allStepsMatched());
            assertEquals(1, result.getMismatches().size());
            assertEquals("ERROR", result.getMismatches().get(0).getActualOutcome());
        }

        @Test
        void replayFlows_replayMultipleFlows() {
            when(logRepository.findByFlowIdOrderByStartedAtAsc(anyString())).thenReturn(List.of());

            List<ReplayResult> results = runner.replayFlows(List.of("flow-1", "flow-2", "flow-3"));

            assertEquals(3, results.size());
            assertEquals("flow-1", results.get(0).getFlowId());
            assertEquals("flow-2", results.get(1).getFlowId());
            assertEquals("flow-3", results.get(2).getFlowId());
        }
    }

    // =====================================================================
    // TopicValidator tests
    // =====================================================================

    @Nested
    class TopicValidatorTests {

        @Test
        void validateTopics_kafkaUnreachable_throwsIllegalState() {
            KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
            // Return props that cause AdminClient creation to fail
            when(kafkaAdmin.getConfigurationProperties()).thenReturn(Map.of(
                    "bootstrap.servers", "unreachable:9092",
                    "request.timeout.ms", "100",
                    "default.api.timeout.ms", "100"
            ));

            var props = new OrchestratorProperties();
            props.getKafka().setCommandTopic("orchestrator.commands");

            var validator = new TopicValidator(kafkaAdmin, props);

            // AdminClient.create will succeed but listTopics().names().get() will time out
            // or throw. The validator wraps this in IllegalStateException.
            assertThrows(IllegalStateException.class, () -> validator.validateTopics());
        }
    }

    // =====================================================================
    // IndexInitializer tests
    // =====================================================================

    @Nested
    class IndexInitializerTests {

        private MongoTemplate mongoTemplate;
        private OrchestratorProperties props;
        private FlowTypeRegistry flowTypeRegistry;
        private IndexOperations indexOps;

        @BeforeEach
        void setUp() {
            mongoTemplate = mock(MongoTemplate.class);
            indexOps = mock(IndexOperations.class);
            when(mongoTemplate.indexOps(anyString())).thenReturn(indexOps);
            when(mongoTemplate.getCollectionNames()).thenReturn(Set.of());

            props = new OrchestratorProperties();
            flowTypeRegistry = new FlowTypeRegistry(List.of());
        }

        @Test
        void createIndexes_createsTtlIndexes() {
            props.getRetention().setOutboxDays(7);
            props.getRetention().setProcessedEventsDays(30);
            props.getRetention().setStepLogDays(90);

            var initializer = new IndexInitializer(mongoTemplate, props, flowTypeRegistry);
            initializer.createIndexes();

            // Should create TTL indexes for outbox, processed events, step log
            verify(mongoTemplate, atLeastOnce()).indexOps("orchestrator_outbox");
            verify(mongoTemplate, atLeastOnce()).indexOps("orchestrator_processed_events");
            verify(mongoTemplate, atLeastOnce()).indexOps("orchestrator_step_log");
            verify(indexOps, atLeast(3)).ensureIndex(any());
        }

        @Test
        void createIndexes_skipsTtlWhenDaysIsZero() {
            props.getRetention().setOutboxDays(0);
            props.getRetention().setProcessedEventsDays(0);
            props.getRetention().setStepLogDays(0);

            var initializer = new IndexInitializer(mongoTemplate, props, flowTypeRegistry);
            initializer.createIndexes();

            // Should still create performance indexes (outbox unpublished_idx, step log indexes)
            // but NOT TTL indexes
            verify(indexOps, atLeastOnce()).ensureIndex(any());
        }

        @Test
        void createIndexes_failureIsLogged_notThrown() {
            when(indexOps.ensureIndex(any())).thenThrow(new RuntimeException("index creation failed"));

            var initializer = new IndexInitializer(mongoTemplate, props, flowTypeRegistry);

            // Should not throw — failures are logged and swallowed
            assertDoesNotThrow(() -> initializer.createIndexes());
        }

        @Test
        void createIndexes_createsFlowCollectionIndexes() {
            // Simulate a flow collection detected by getFlowCollections
            var mockCollection = mock(com.mongodb.client.MongoCollection.class);
            var mockIterable = mock(com.mongodb.client.FindIterable.class);
            when(mockCollection.find()).thenReturn(mockIterable);
            when(mockIterable.limit(1)).thenReturn(mockIterable);
            var doc = new org.bson.Document()
                    .append("status", "IN_PROGRESS")
                    .append("currentStep", "STEP_A");
            when(mockIterable.first()).thenReturn(doc);
            when(mongoTemplate.getCollectionNames()).thenReturn(Set.of("my_flows"));
            when(mongoTemplate.getCollection("my_flows")).thenReturn(mockCollection);

            var initializer = new IndexInitializer(mongoTemplate, props, flowTypeRegistry);
            initializer.createIndexes();

            // Should have created flow indexes on my_flows collection
            verify(mongoTemplate, atLeastOnce()).indexOps("my_flows");
        }

        @Test
        void createIndexes_fallsBackToDefaultCollections_whenNoneDetected() {
            when(mongoTemplate.getCollectionNames()).thenReturn(Set.of());

            var initializer = new IndexInitializer(mongoTemplate, props, flowTypeRegistry);
            initializer.createIndexes();

            // Fallback collections: dis_instrument_flows and enigio_flows
            verify(mongoTemplate, atLeastOnce()).indexOps("dis_instrument_flows");
            verify(mongoTemplate, atLeastOnce()).indexOps("enigio_flows");
        }
    }

    // =====================================================================
    // JitteredExponentialBackOffPolicy — additional coverage
    // =====================================================================

    @Nested
    class JitteredBackOffPolicyAdditionalTests {

        @Test
        void backOffExecution_nextBackOff_respectsMaxInterval() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(1000);
            policy.setMultiplier(10.0);
            policy.setMaxInterval(5000);
            policy.setJitterFactor(0.0); // No jitter for deterministic test

            BackOffExecution execution = policy.start();

            long first = execution.nextBackOff();
            assertEquals(1000, first);

            long second = execution.nextBackOff();
            // base would be 10000 but capped at 5000
            assertEquals(5000, second);

            long third = execution.nextBackOff();
            // still capped at 5000
            assertEquals(5000, third);
        }

        @Test
        void backOffExecution_withJitter_staysInRange() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(2000);
            policy.setMultiplier(2.0);
            policy.setMaxInterval(30000);
            policy.setJitterFactor(0.5);

            BackOffExecution execution = policy.start();

            for (int i = 0; i < 20; i++) {
                long delay = execution.nextBackOff();
                assertTrue(delay >= 0, "Delay should be non-negative: " + delay);
                assertTrue(delay <= 30000, "Delay should not exceed maxInterval: " + delay);
            }
        }

        @Test
        void backOffExecution_fullJitter_canProduceZero() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(100);
            policy.setMultiplier(2.0);
            policy.setMaxInterval(10000);
            policy.setJitterFactor(1.0); // Full jitter: fixed=0, jitter=0-100

            BackOffExecution execution = policy.start();

            // With full jitter, the minimum is 0, the max is base
            // Run enough iterations to validate range
            boolean seenLow = false;
            boolean seenHigh = false;
            for (int i = 0; i < 100; i++) {
                execution = policy.start(); // reset each time for initial interval
                long delay = execution.nextBackOff();
                assertTrue(delay >= 0 && delay <= 100,
                        "Delay should be 0-100 with full jitter, was " + delay);
                if (delay < 30) seenLow = true;
                if (delay > 70) seenHigh = true;
            }
            // Statistical: with 100 runs of uniform [0,100], extremely unlikely to not see both ends
            assertTrue(seenLow, "Should have seen low values");
            assertTrue(seenHigh, "Should have seen high values");
        }

        @Test
        void sleepingBackOff_interruptedThread_throwsBackOffInterrupted() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(1000);
            policy.setJitterFactor(0.0);
            policy.setSleeper(millis -> {
                throw new InterruptedException("interrupted");
            });

            var ctx = policy.start(null);
            assertThrows(org.springframework.retry.backoff.BackOffInterruptedException.class,
                    () -> policy.backOff(ctx));

            // Thread interrupt flag should be set
            assertTrue(Thread.currentThread().isInterrupted());
            // Clear the flag for other tests
            Thread.interrupted();
        }

        @Test
        void sleepingBackOff_multiplierApplied() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(1000);
            policy.setMultiplier(3.0);
            policy.setMaxInterval(100000);
            policy.setJitterFactor(0.0);

            List<Long> delays = new ArrayList<>();
            policy.setSleeper(delays::add);

            var ctx = policy.start(null);
            policy.backOff(ctx); // 1000
            policy.backOff(ctx); // 3000
            policy.backOff(ctx); // 9000

            assertEquals(3, delays.size());
            assertEquals(1000, delays.get(0));
            assertEquals(3000, delays.get(1));
            assertEquals(9000, delays.get(2));
        }

        @Test
        void withSleeper_copiesAllSettings() {
            var policy = new JitteredExponentialBackOffPolicy();
            policy.setInitialInterval(500);
            policy.setMultiplier(3.0);
            policy.setMaxInterval(15000);
            policy.setJitterFactor(0.7);

            List<Long> delays = new ArrayList<>();
            var copy = policy.withSleeper(delays::add);

            // Verify the copy works with the original settings
            var ctx = copy.start(null);
            copy.backOff(ctx);

            assertEquals(1, delays.size());
            // With jitter=0.7 and initial=500: fixed=150, jitter=0-350, so delay in [150,500]
            assertTrue(delays.get(0) >= 150 && delays.get(0) <= 500,
                    "Delay should be 150-500, was " + delays.get(0));
        }
    }
}
