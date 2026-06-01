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
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

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

    // --- Step handlers for new test scenarios ---

    /** No annotations at all — exercises built-in defaults */
    static class UnannotatedStep implements StepHandler<TestFlow> {
        public String getStepName() { return "UNANNOTATED"; }
        public int getOrder() { return 1; }
        public void execute(TestFlow f) {}
    }

    @RecoverOn(httpStatus = 422, message = "already signed", action = RecoverAction.SKIP)
    static class RecoverWithMessageStep implements StepHandler<TestFlow> {
        public String getStepName() { return "RECOVER_MSG"; }
        public int getOrder() { return 1; }
        public void execute(TestFlow f) {}
    }

    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    @RecoverOn(httpStatus = 422, message = "duplicate", action = RecoverAction.SKIP_AND_EXTRACT)
    static class MultiRecoverStep implements StepHandler<TestFlow> {
        public String getStepName() { return "MULTI_RECOVER"; }
        public int getOrder() { return 1; }
        public void execute(TestFlow f) {}
    }

    @FailOn(httpStatus = {400, 403})
    @RetryOn(httpStatus = {502, 503})
    static class HttpAnnotatedStep implements StepHandler<TestFlow> {
        public String getStepName() { return "HTTP_ANN"; }
        public int getOrder() { return 1; }
        public void execute(TestFlow f) {}
    }

    // =====================================================================
    // 1. extractHttpStatus with real Spring HTTP exceptions
    // =====================================================================

    @Test
    void httpClientErrorException_extractsStatusAndDefaultNonRetryable() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(400), "Bad Request", null, null, null);
        var thrown = assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
        assertTrue(thrown.getMessage().contains("HTTP 400"));
    }

    @Test
    void httpClientErrorException404_nonRetryableByDefault() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(404), "Not Found", null, null, null);
        var thrown = assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
        assertTrue(thrown.getMessage().contains("HTTP 404"));
    }

    @Test
    void httpClientErrorException429_retryableByDefault() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(429), "Too Many Requests", null, null, null);
        var thrown = assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
        assertTrue(thrown.getMessage().contains("HTTP 429"));
    }

    @Test
    void httpServerErrorException500_retryableByDefault() {
        var handler = new UnannotatedStep();
        var ex = HttpServerErrorException.create(
                HttpStatusCode.valueOf(500), "Internal Server Error", null, null, null);
        var thrown = assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
        assertTrue(thrown.getMessage().contains("HTTP 500"));
    }

    @Test
    void httpServerErrorException502_retryableByDefault() {
        var handler = new UnannotatedStep();
        var ex = HttpServerErrorException.create(
                HttpStatusCode.valueOf(502), "Bad Gateway", null, null, null);
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void httpServerErrorException503_retryableByDefault() {
        var handler = new UnannotatedStep();
        var ex = HttpServerErrorException.create(
                HttpStatusCode.valueOf(503), "Service Unavailable", null, null, null);
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    // =====================================================================
    // 2. @RecoverOn with message matching
    // =====================================================================

    @Test
    void recoverOn_matchesWhenMessageContainsSubstring() {
        var handler = new RecoverWithMessageStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(422), "already signed by notary", null, null, null);
        // Should recover (return normally) — no exception thrown
        assertDoesNotThrow(() -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void recoverOn_doesNotMatchWhenMessageMissing() {
        var handler = new RecoverWithMessageStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(422), "validation failed", null, null, null);
        // Message does not contain "already signed", so should NOT recover
        // Falls through to defaults: 422 is 4xx → non-retryable
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void recoverOn_doesNotMatchWhenStatusDiffers() {
        var handler = new RecoverWithMessageStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(409), "already signed", null, null, null);
        // Status 409 does not match the annotation's 422
        // Falls through to defaults: 409 is 4xx → non-retryable
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    // =====================================================================
    // 3. @RecoverOn SKIP action vs SKIP_AND_EXTRACT action
    // =====================================================================

    @Test
    void recoverOn_skipAction_recoversNormally() {
        var handler = new MultiRecoverStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(409), "Conflict", null, null, null);
        // @RecoverOn(httpStatus = 409, action = SKIP) — should recover
        assertDoesNotThrow(() -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void recoverOn_skipAndExtractAction_recoversNormally() {
        var handler = new MultiRecoverStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(422), "duplicate entry found", null, null, null);
        // @RecoverOn(httpStatus = 422, message = "duplicate", action = SKIP_AND_EXTRACT)
        assertDoesNotThrow(() -> StepErrorHandler.handleError(handler, ex));
    }

    // =====================================================================
    // 4. Default behavior for HTTP 4xx vs 5xx with no annotations
    // =====================================================================

    @Test
    void default_http401_nonRetryable() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(401), "Unauthorized", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void default_http403_nonRetryable() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(403), "Forbidden", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void default_http409_nonRetryable() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(409), "Conflict", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void default_http422_nonRetryable() {
        var handler = new UnannotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(422), "Unprocessable Entity", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    // =====================================================================
    // 5. Chained exceptions (cause chain traversal)
    // =====================================================================

    @Test
    void extractHttpStatus_traversesCauseChain() {
        var handler = new UnannotatedStep();
        var httpEx = HttpServerErrorException.create(
                HttpStatusCode.valueOf(503), "Service Unavailable", null, null, null);
        // Wrap the HTTP exception inside a generic RuntimeException
        var wrappedEx = new RuntimeException("wrapper", httpEx);
        var thrown = assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, wrappedEx));
        assertTrue(thrown.getMessage().contains("HTTP 503"));
    }

    @Test
    void extractHttpStatus_traversesDeeplyNestedCauseChain() {
        var handler = new UnannotatedStep();
        var httpEx = HttpClientErrorException.create(
                HttpStatusCode.valueOf(404), "Not Found", null, null, null);
        var level1 = new RuntimeException("level1", httpEx);
        var level2 = new IllegalStateException("level2", level1);
        var thrown = assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, level2));
        assertTrue(thrown.getMessage().contains("HTTP 404"));
    }

    @Test
    void failOn_matchesCauseException() {
        var handler = new AnnotatedStep();
        // @FailOn(exceptions = {IllegalArgumentException.class})
        // Wrap IllegalArgumentException as the cause of a RuntimeException
        var cause = new IllegalArgumentException("bad");
        var wrapper = new RuntimeException("wrapped", cause);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, wrapper));
    }

    @Test
    void retryOn_matchesCauseException() {
        var handler = new AnnotatedStep();
        // @RetryOn(exceptions = {IllegalStateException.class})
        // Wrap IllegalStateException as the cause of a RuntimeException
        var cause = new IllegalStateException("transient");
        var wrapper = new RuntimeException("wrapped", cause);
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, wrapper));
    }

    // =====================================================================
    // 6. extractHttpStatus returns 0 for non-HTTP exceptions
    // =====================================================================

    @Test
    void nonHttpException_noAnnotations_defaultsToRetryable() {
        var handler = new UnannotatedStep();
        // A plain RuntimeException has no getStatusCode() — extractHttpStatus returns 0
        // With no annotations and no HTTP status, falls through to default retryable
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new RuntimeException("plain error")));
    }

    @Test
    void nonHttpException_npe_defaultsToRetryable() {
        var handler = new UnannotatedStep();
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new NullPointerException("oops")));
    }

    @Test
    void nonHttpException_ioException_defaultsToRetryable() {
        var handler = new UnannotatedStep();
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, new java.io.IOException("network down")));
    }

    // =====================================================================
    // Annotation-based HTTP status matching with real HTTP exceptions
    // =====================================================================

    @Test
    void failOn_httpStatus_matchesRealHttpException() {
        var handler = new HttpAnnotatedStep();
        // @FailOn(httpStatus = {400, 403})
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(400), "Bad Request", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void failOn_httpStatus403_matchesRealHttpException() {
        var handler = new HttpAnnotatedStep();
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(403), "Forbidden", null, null, null);
        assertThrows(NonRetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void retryOn_httpStatus_matchesRealHttpException() {
        var handler = new HttpAnnotatedStep();
        // @RetryOn(httpStatus = {502, 503})
        var ex = HttpServerErrorException.create(
                HttpStatusCode.valueOf(502), "Bad Gateway", null, null, null);
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void retryOn_httpStatus503_matchesRealHttpException() {
        var handler = new HttpAnnotatedStep();
        var ex = HttpServerErrorException.create(
                HttpStatusCode.valueOf(503), "Service Unavailable", null, null, null);
        assertThrows(RetryableStepException.class,
                () -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void recoverOn_withRealHttpClientErrorException409() {
        var handler = new AnnotatedStep();
        // @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP) on AnnotatedStep
        var ex = HttpClientErrorException.create(
                HttpStatusCode.valueOf(409), "Conflict", null, null, null);
        assertDoesNotThrow(() -> StepErrorHandler.handleError(handler, ex));
    }

    @Test
    void recoverOn_wrappedHttpException_recovers() {
        var handler = new AnnotatedStep();
        // @RecoverOn(httpStatus = 409) on AnnotatedStep
        var httpEx = HttpClientErrorException.create(
                HttpStatusCode.valueOf(409), "Conflict", null, null, null);
        var wrapped = new RuntimeException("call failed", httpEx);
        // extractHttpStatus traverses the cause chain and finds 409
        assertDoesNotThrow(() -> StepErrorHandler.handleError(handler, wrapped));
    }
}
