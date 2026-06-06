package com.orchestrator.starter;

import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.StepHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for StepHandler default methods and OrchestratorFlow default methods.
 */
@DisplayName("StepHandler & OrchestratorFlow defaults")
class StepHandlerDefaultsTest {

    // ========== Test flow for OrchestratorFlow defaults ==========

    static class MinimalFlow implements OrchestratorFlow {
        private String id = "flow-1";
        private String correlationId = "corr-1";
        private String currentStep;
        private FlowStatus status = FlowStatus.PENDING;
        private int retryCount;
        private int backoffSeconds;
        private Instant nextRetryAt;
        private String errorMessage;
        private Instant updatedAt;

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

    // ========== StepHandler with @Step annotation ==========

    @Step(name = "CREATE_DOC", order = 5)
    static class AnnotatedStep implements StepHandler<MinimalFlow> {
        @Override
        public void execute(MinimalFlow flow) {}
    }

    // ========== StepHandler with @Step but empty name ==========

    @Step(name = "", order = 3)
    static class AnnotatedStepEmptyName implements StepHandler<MinimalFlow> {
        @Override
        public void execute(MinimalFlow flow) {}
    }

    // ========== StepHandler without annotation and no override ==========

    static class UnannotatedStep implements StepHandler<MinimalFlow> {
        @Override
        public void execute(MinimalFlow flow) {}
    }

    // ========== StepHandler with manual override ==========

    static class ManualStep implements StepHandler<MinimalFlow> {
        @Override
        public String getStepName() { return "MANUAL_STEP"; }
        @Override
        public int getOrder() { return 42; }
        @Override
        public void execute(MinimalFlow flow) {}
    }

    // =====================================================================
    // StepHandler tests
    // =====================================================================

    @Nested
    @DisplayName("StepHandler.getStepName()")
    class GetStepNameTests {

        @Test
        @DisplayName("returns name from @Step annotation")
        void annotated_returnsAnnotationName() {
            var step = new AnnotatedStep();
            assertThat(step.getStepName()).isEqualTo("CREATE_DOC");
        }

        @Test
        @DisplayName("returns empty string from @Step with empty name")
        void annotatedEmptyName_returnsEmptyString() {
            var step = new AnnotatedStepEmptyName();
            assertThat(step.getStepName()).isEmpty();
        }

        @Test
        @DisplayName("throws when no annotation and no override")
        void unannotated_throwsIllegalState() {
            var step = new UnannotatedStep();
            assertThatThrownBy(step::getStepName)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("UnannotatedStep")
                    .hasMessageContaining("override getStepName()");
        }

        @Test
        @DisplayName("returns overridden name")
        void manual_returnsOverriddenName() {
            var step = new ManualStep();
            assertThat(step.getStepName()).isEqualTo("MANUAL_STEP");
        }
    }

    @Nested
    @DisplayName("StepHandler.getOrder()")
    class GetOrderTests {

        @Test
        @DisplayName("returns order from @Step annotation")
        void annotated_returnsAnnotationOrder() {
            var step = new AnnotatedStep();
            assertThat(step.getOrder()).isEqualTo(5);
        }

        @Test
        @DisplayName("throws when no annotation and no override")
        void unannotated_throwsIllegalState() {
            var step = new UnannotatedStep();
            assertThatThrownBy(step::getOrder)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("UnannotatedStep")
                    .hasMessageContaining("override getOrder()");
        }

        @Test
        @DisplayName("returns overridden order")
        void manual_returnsOverriddenOrder() {
            var step = new ManualStep();
            assertThat(step.getOrder()).isEqualTo(42);
        }
    }

    // =====================================================================
    // OrchestratorFlow default methods
    // =====================================================================

    @Nested
    @DisplayName("OrchestratorFlow default methods")
    class OrchestratorFlowDefaultsTests {

        private final MinimalFlow flow = new MinimalFlow();

        @Test
        @DisplayName("getFlowType returns null by default")
        void flowType_defaultNull() {
            assertThat(flow.getFlowType()).isNull();
        }

        @Test
        @DisplayName("setFlowType is a no-op by default")
        void setFlowType_noOp() {
            flow.setFlowType("test");
            assertThat(flow.getFlowType()).isNull();
        }

        @Test
        @DisplayName("getCompletedParallelSteps returns empty set")
        void completedParallelSteps_emptySet() {
            assertThat(flow.getCompletedParallelSteps()).isEmpty();
        }

        @Test
        @DisplayName("setCompletedParallelSteps is a no-op by default")
        void setCompletedParallelSteps_noOp() {
            flow.setCompletedParallelSteps(java.util.Set.of("STEP_A"));
            assertThat(flow.getCompletedParallelSteps()).isEmpty();
        }

        @Test
        @DisplayName("getRecoveryCount returns 0")
        void recoveryCount_zero() {
            assertThat(flow.getRecoveryCount()).isZero();
        }

        @Test
        @DisplayName("setRecoveryCount is a no-op")
        void setRecoveryCount_noOp() {
            flow.setRecoveryCount(5);
            assertThat(flow.getRecoveryCount()).isZero();
        }

        @Test
        @DisplayName("getPollCount returns 0")
        void pollCount_zero() {
            assertThat(flow.getPollCount()).isZero();
        }

        @Test
        @DisplayName("setPollCount is a no-op")
        void setPollCount_noOp() {
            flow.setPollCount(10);
            assertThat(flow.getPollCount()).isZero();
        }

        @Test
        @DisplayName("getCompensationError returns null")
        void compensationError_null() {
            assertThat(flow.getCompensationError()).isNull();
        }

        @Test
        @DisplayName("getWaitingSince returns null")
        void waitingSince_null() {
            assertThat(flow.getWaitingSince()).isNull();
        }

        @Test
        @DisplayName("getExpiresAt returns null")
        void expiresAt_null() {
            assertThat(flow.getExpiresAt()).isNull();
        }

        @Test
        @DisplayName("getSleepUntil returns null")
        void sleepUntil_null() {
            assertThat(flow.getSleepUntil()).isNull();
        }

        @Test
        @DisplayName("getPendingSignals returns null")
        void pendingSignals_null() {
            assertThat(flow.getPendingSignals()).isNull();
        }

        @Test
        @DisplayName("getChildFlowIds returns null")
        void childFlowIds_null() {
            assertThat(flow.getChildFlowIds()).isNull();
        }

        @Test
        @DisplayName("getParentFlowId returns null")
        void parentFlowId_null() {
            assertThat(flow.getParentFlowId()).isNull();
        }

        @Test
        @DisplayName("getParentFlowType returns null")
        void parentFlowType_null() {
            assertThat(flow.getParentFlowType()).isNull();
        }

        @Test
        @DisplayName("getParentStepName returns null")
        void parentStepName_null() {
            assertThat(flow.getParentStepName()).isNull();
        }

        @Test
        @DisplayName("getCompletedSteps returns empty set")
        void completedSteps_emptySet() {
            assertThat(flow.getCompletedSteps()).isEmpty();
        }

        @Test
        @DisplayName("getClaimedBy returns null")
        void claimedBy_null() {
            assertThat(flow.getClaimedBy()).isNull();
        }

        @Test
        @DisplayName("getClaimedAt returns null")
        void claimedAt_null() {
            assertThat(flow.getClaimedAt()).isNull();
        }

        @Test
        @DisplayName("getExecutingStep returns null")
        void executingStep_null() {
            assertThat(flow.getExecutingStep()).isNull();
        }

        @Test
        @DisplayName("getExecutingPod returns null")
        void executingPod_null() {
            assertThat(flow.getExecutingPod()).isNull();
        }

        @Test
        @DisplayName("setCorrelationId is a no-op by default")
        void setCorrelationId_noOp() {
            // The default implementation is empty — just verify it doesn't throw
            flow.setCorrelationId("new-corr");
            // MinimalFlow overrides this, so check the interface default
            OrchestratorFlow interfaceOnly = new OrchestratorFlow() {
                public String getId() { return "x"; }
                public String getCorrelationId() { return null; }
                public String getCurrentStep() { return null; }
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
                public Instant getUpdatedAt() { return null; }
                public void setUpdatedAt(Instant i) {}
            };
            // These are all no-op defaults — just verify no exception
            interfaceOnly.setCorrelationId("test");
            interfaceOnly.setFlowType("test");
            interfaceOnly.setCompletedParallelSteps(java.util.Set.of());
            interfaceOnly.setRecoveryCount(1);
            interfaceOnly.setPollCount(1);
            interfaceOnly.setCompensationError("err");
            interfaceOnly.setWaitingSince(Instant.now());
            interfaceOnly.setExpiresAt(Instant.now());
            interfaceOnly.setSleepUntil(Instant.now());
            interfaceOnly.setPendingSignals(java.util.List.of());
            interfaceOnly.setChildFlowIds(java.util.List.of());
            interfaceOnly.setParentFlowId("p");
            interfaceOnly.setParentFlowType("pt");
            interfaceOnly.setParentStepName("ps");
            interfaceOnly.setCompletedSteps(java.util.Set.of());
            interfaceOnly.setClaimedBy("pod");
            interfaceOnly.setClaimedAt(Instant.now());
            interfaceOnly.setExecutingStep("s");
            interfaceOnly.setExecutingPod("p");
        }
    }
}
