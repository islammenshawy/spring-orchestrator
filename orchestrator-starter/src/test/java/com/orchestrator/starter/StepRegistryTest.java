package com.orchestrator.starter;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StepRegistryTest {

    static class TestFlow implements OrchestratorFlow {
        private String id = "test-1";
        private String correlationId = "corr-1";
        private String currentStep = "STEP_A";
        private FlowStatus status = FlowStatus.PENDING;
        private int retryCount = 0;
        private int backoffSeconds = 0;
        private Instant nextRetryAt;
        private String errorMessage;
        private Instant updatedAt = Instant.now();
        private Set<String> completedParallelSteps = Set.of();

        public String getId() { return id; }
        public String getCorrelationId() { return correlationId; }
        public String getCurrentStep() { return currentStep; }
        public void setCurrentStep(String s) { currentStep = s; }
        public FlowStatus getStatus() { return status; }
        public void setStatus(FlowStatus s) { status = s; }
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int c) { retryCount = c; }
        public int getBackoffSeconds() { return backoffSeconds; }
        public void setBackoffSeconds(int s) { backoffSeconds = s; }
        public Instant getNextRetryAt() { return nextRetryAt; }
        public void setNextRetryAt(Instant i) { nextRetryAt = i; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String m) { errorMessage = m; }
        public Instant getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(Instant i) { updatedAt = i; }
        public Set<String> getCompletedParallelSteps() { return completedParallelSteps; }
        public void setCompletedParallelSteps(Set<String> s) { completedParallelSteps = s; }
    }

    static StepHandler<TestFlow> step(String name, int order) {
        return new StepHandler<>() {
            public String getStepName() { return name; }
            public int getOrder() { return order; }
            public boolean isAlreadyCompleted(TestFlow f) { return false; }
            public void execute(TestFlow f) {}
        };
    }

    @Test
    void registersStepsInOrder() {
        var registry = new StepRegistry<>(List.of(step("C", 3), step("A", 1), step("B", 2)));
        assertEquals(List.of("A", "B", "C"), registry.getStepNames());
    }

    @Test
    void firstStep() {
        var registry = new StepRegistry<>(List.of(step("B", 2), step("A", 1)));
        assertEquals("A", registry.getFirstStep());
    }

    @Test
    void nextStep() {
        var registry = new StepRegistry<>(List.of(step("A", 1), step("B", 2), step("C", 3)));
        assertEquals("B", registry.getNextStep("A"));
        assertEquals("C", registry.getNextStep("B"));
        assertNull(registry.getNextStep("C"));
    }

    @Test
    void completedStepsBefore() {
        var registry = new StepRegistry<>(List.of(step("A", 1), step("B", 2), step("C", 3)));
        assertEquals(List.of(), registry.getCompletedStepsBefore("A"));
        assertEquals(List.of("A"), registry.getCompletedStepsBefore("B"));
        assertEquals(List.of("A", "B"), registry.getCompletedStepsBefore("C"));
    }

    @Test
    void getHandlerThrowsForUnknown() {
        var registry = new StepRegistry<>(List.of(step("A", 1)));
        assertThrows(IllegalArgumentException.class, () -> registry.getHandler("UNKNOWN"));
    }
}
