package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorHealthIndicator;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetricsAndHealthTest {

    // Minimal OrchestratorFlow stub for type bounds
    static class StubFlow implements OrchestratorFlow {
        public String getId() { return "s"; }
        public String getCorrelationId() { return "c"; }
        public String getCurrentStep() { return "S"; }
        public void setCurrentStep(String s) {}
        public FlowStatus getStatus() { return FlowStatus.PENDING; }
        public void setStatus(FlowStatus s) {}
        public int getRetryCount() { return 0; }
        public void setRetryCount(int c) {}
        public int getBackoffSeconds() { return 0; }
        public void setBackoffSeconds(int s) {}
        public Instant getNextRetryAt() { return null; }
        public void setNextRetryAt(Instant i) {}
        public String getErrorMessage() { return null; }
        public void setErrorMessage(String m) {}
        public Instant getUpdatedAt() { return Instant.now(); }
        public void setUpdatedAt(Instant i) {}
    }

    // =====================================================================
    // OrchestratorMetrics tests
    // =====================================================================

    @Nested
    class MetricsTests {

        private MeterRegistry registry;
        private OrchestratorMetrics metrics;

        @BeforeEach
        void setUp() {
            registry = new SimpleMeterRegistry();
            metrics = new OrchestratorMetrics(registry);
        }

        @Test
        void noop_returnsInstanceThatDoesNotThrow() {
            OrchestratorMetrics noop = OrchestratorMetrics.noop();
            assertFalse(noop.isEnabled());

            // All methods should be no-ops — no exceptions
            assertDoesNotThrow(() -> noop.flowStarted("test"));
            assertDoesNotThrow(() -> noop.flowCompleted("test"));
            assertDoesNotThrow(() -> noop.flowFailed("test"));
            assertDoesNotThrow(() -> noop.compensationFailed("test"));
            assertDoesNotThrow(() -> noop.stepExecution("test", "STEP", "COMPLETED", Duration.ofMillis(100)));
            assertDoesNotThrow(() -> noop.outboxPublished());
            assertDoesNotThrow(() -> noop.outboxDeadLettered());
            assertDoesNotThrow(() -> noop.recoveryRecovered("test"));
            assertDoesNotThrow(() -> noop.idempotencyDuplicate());
        }

        @Test
        void noop_returnsSameInstance() {
            assertSame(OrchestratorMetrics.noop(), OrchestratorMetrics.noop());
        }

        @Test
        void isEnabled_trueWithRegistry() {
            assertTrue(metrics.isEnabled());
        }

        @Test
        void isEnabled_falseWithNullRegistry() {
            OrchestratorMetrics noopMetrics = new OrchestratorMetrics(null);
            assertFalse(noopMetrics.isEnabled());
        }

        @Test
        void flowStarted_incrementsCounter() {
            metrics.flowStarted("payment");
            metrics.flowStarted("payment");

            double count = registry.counter("orchestrator.flows.started", "flowType", "payment").count();
            assertEquals(2.0, count);
        }

        @Test
        void flowCompleted_incrementsCounter() {
            metrics.flowCompleted("order");

            double count = registry.counter("orchestrator.flows.completed", "flowType", "order").count();
            assertEquals(1.0, count);
        }

        @Test
        void flowFailed_incrementsCounter() {
            metrics.flowFailed("order");

            double count = registry.counter("orchestrator.flows.failed", "flowType", "order").count();
            assertEquals(1.0, count);
        }

        @Test
        void compensationFailed_incrementsCounter() {
            metrics.compensationFailed("payment");

            double count = registry.counter("orchestrator.compensation.failed", "flowType", "payment").count();
            assertEquals(1.0, count);
        }

        @Test
        void stepExecution_recordsTimer() {
            metrics.stepExecution("payment", "CREATE_DOC", "COMPLETED", Duration.ofMillis(250));

            var timer = registry.timer("orchestrator.step.executions",
                    "flowType", "payment", "stepName", "CREATE_DOC", "outcome", "COMPLETED");
            assertEquals(1, timer.count());
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) >= 250);
        }

        @Test
        void outboxPublished_incrementsCounter() {
            metrics.outboxPublished();
            metrics.outboxPublished();
            metrics.outboxPublished();

            double count = registry.counter("orchestrator.outbox.published").count();
            assertEquals(3.0, count);
        }

        @Test
        void outboxDeadLettered_incrementsCounter() {
            metrics.outboxDeadLettered();

            double count = registry.counter("orchestrator.outbox.dead_lettered").count();
            assertEquals(1.0, count);
        }

        @Test
        void recoveryRecovered_incrementsCounter() {
            metrics.recoveryRecovered("order");

            double count = registry.counter("orchestrator.recovery.recovered", "flowType", "order").count();
            assertEquals(1.0, count);
        }

        @Test
        void idempotencyDuplicate_incrementsCounter() {
            metrics.idempotencyDuplicate();

            double count = registry.counter("orchestrator.idempotency.duplicates").count();
            assertEquals(1.0, count);
        }

        @Test
        void flowStarted_nullFlowType_usesDefault() {
            metrics.flowStarted(null);

            double count = registry.counter("orchestrator.flows.started", "flowType", "default").count();
            assertEquals(1.0, count);
        }

        @Test
        void stepExecution_nullFlowType_usesDefault() {
            metrics.stepExecution(null, "STEP_A", "FAILED", Duration.ofMillis(50));

            var timer = registry.timer("orchestrator.step.executions",
                    "flowType", "default", "stepName", "STEP_A", "outcome", "FAILED");
            assertEquals(1, timer.count());
        }
    }

    // =====================================================================
    // OrchestratorHealthIndicator tests
    // =====================================================================

    @Nested
    class HealthTests {

        private OutboxEventRepository outboxRepo;
        private FlowTypeRegistry flowTypeRegistry;

        @BeforeEach
        void setUp() {
            outboxRepo = mock(OutboxEventRepository.class);
        }

        private OrchestratorHealthIndicator createIndicator(int outboxThreshold, int staleMinutes) {
            return new OrchestratorHealthIndicator(outboxRepo, flowTypeRegistry, outboxThreshold, staleMinutes);
        }

        @Test
        void health_up_whenAllClear() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(0L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            flowTypeRegistry = new FlowTypeRegistry(List.of());

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            assertEquals(Status.UP, health.getStatus());
            assertEquals(0L, health.getDetails().get("outboxPending"));
            assertEquals(0L, health.getDetails().get("outboxDeadLettered"));
            assertEquals(100, health.getDetails().get("outboxThreshold"));
            assertEquals(0L, health.getDetails().get("staleFlows"));
        }

        @Test
        void health_down_whenPendingExceedsThreshold() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(150L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            flowTypeRegistry = new FlowTypeRegistry(List.of());

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            assertEquals(Status.DOWN, health.getStatus());
            assertEquals(150L, health.getDetails().get("outboxPending"));
        }

        @Test
        void health_down_whenDeadLetteredEventsExist() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(0L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(3L);

            flowTypeRegistry = new FlowTypeRegistry(List.of());

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            assertEquals(Status.DOWN, health.getStatus());
            assertEquals(3L, health.getDetails().get("outboxDeadLettered"));
        }

        @Test
        void health_up_withStaleFlows_staleFlowsDoNotCauseDown() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(0L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            @SuppressWarnings("unchecked")
            OrchestratorFlowRepository<StubFlow> flowRepo = mock(OrchestratorFlowRepository.class);
            when(flowRepo.countByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any(Instant.class)))
                    .thenReturn(5L);
            when(flowRepo.countByStatusAndUpdatedAtBefore(eq(FlowStatus.COMPENSATION_FAILED), any(Instant.class)))
                    .thenReturn(2L);

            var desc = FlowTypeDescriptor.builder()
                    .flowType("test")
                    .entityClass(StubFlow.class)
                    .commandTopic("t")
                    .replyTopic("r")
                    .dltTopic("d")
                    .build();
            desc.setRepository(flowRepo);

            flowTypeRegistry = new FlowTypeRegistry(List.of(desc));

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            // Stale flows do NOT cause DOWN — only outbox issues do
            assertEquals(Status.UP, health.getStatus());
            assertEquals(7L, health.getDetails().get("staleFlows"));
        }

        @Test
        void health_skipsDescriptorWithNullRepository() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(0L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            var desc = FlowTypeDescriptor.builder()
                    .flowType("test")
                    .entityClass(StubFlow.class)
                    .commandTopic("t")
                    .replyTopic("r")
                    .dltTopic("d")
                    .build();
            // repository is null — should be skipped without error

            flowTypeRegistry = new FlowTypeRegistry(List.of(desc));

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            assertEquals(Status.UP, health.getStatus());
            assertEquals(0L, health.getDetails().get("staleFlows"));
        }

        @Test
        void health_pendingAtThreshold_isStillUp() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(100L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            flowTypeRegistry = new FlowTypeRegistry(List.of());

            var indicator = createIndicator(100, 15);
            Health health = indicator.health();

            // pending == threshold is not > threshold, so UP
            assertEquals(Status.UP, health.getStatus());
        }

        @Test
        void health_compensationFailedQueryException_swallowed() {
            when(outboxRepo.countByPublishedFalseAndDeadLetteredFalse()).thenReturn(0L);
            when(outboxRepo.countByDeadLetteredTrue()).thenReturn(0L);

            @SuppressWarnings("unchecked")
            OrchestratorFlowRepository<StubFlow> flowRepo = mock(OrchestratorFlowRepository.class);
            when(flowRepo.countByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any(Instant.class)))
                    .thenReturn(0L);
            when(flowRepo.countByStatusAndUpdatedAtBefore(eq(FlowStatus.COMPENSATION_FAILED), any(Instant.class)))
                    .thenThrow(new RuntimeException("query error"));

            var desc = FlowTypeDescriptor.builder()
                    .flowType("test")
                    .entityClass(StubFlow.class)
                    .commandTopic("t")
                    .replyTopic("r")
                    .dltTopic("d")
                    .build();
            desc.setRepository(flowRepo);

            flowTypeRegistry = new FlowTypeRegistry(List.of(desc));

            var indicator = createIndicator(100, 15);
            // Should not throw — exception is caught
            Health health = indicator.health();
            assertEquals(Status.UP, health.getStatus());
        }
    }
}
