package com.orchestrator.starter;

import com.orchestrator.starter.annotation.FailOn;
import com.orchestrator.starter.annotation.RecoverAction;
import com.orchestrator.starter.annotation.RecoverOn;
import com.orchestrator.starter.annotation.RetryOn;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.StepErrorHandler;
import com.orchestrator.starter.flow.StepHandler;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StepErrorHandlerTest {

    // Mock HTTP exception with status code accessible via reflection
    static class MockHttpException extends RuntimeException {
        private final int statusValue;
        MockHttpException(int status, String msg) {
            super(msg);
            this.statusValue = status;
        }
    }

    static class TestFlow implements OrchestratorFlow {
        public String getId() { return "t"; }
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

    @RetryOn(exceptions = {IllegalStateException.class})
    @FailOn(exceptions = {IllegalArgumentException.class})
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    static class AnnotatedStep implements StepHandler<TestFlow> {
        public String getStepName() { return "TEST"; }
        public int getOrder() { return 1; }
        public void execute(TestFlow f) {}
    }

    @Test
    void retryOnMatchingException() {
        var handler = new AnnotatedStep();
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new IllegalStateException("transient")));
    }

    @Test
    void failOnMatchingException() {
        var handler = new AnnotatedStep();
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new IllegalArgumentException("bad input")));
    }

    @Test
    void defaultToRetryableForUnknownException() {
        var handler = new AnnotatedStep();
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new RuntimeException("unknown")));
    }

    @Test
    void propagateExistingRetryableException() {
        var handler = new AnnotatedStep();
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new RetryableStepException("already typed")));
    }

    @Test
    void propagateExistingNonRetryableException() {
        var handler = new AnnotatedStep();
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new NonRetryableStepException("already typed")));
    }
}
