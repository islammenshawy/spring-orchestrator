package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.FailOn;
import com.orchestrator.starter.annotation.RecoverOn;
import com.orchestrator.starter.annotation.RetryOn;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps exceptions to retry/recover/fail behavior using annotations.
 *
 * Built-in defaults (when no annotations declared):
 * - HTTP 5xx + 429 → retryable (Kafka retry topics)
 * - HTTP 4xx (except 429) → non-retryable (FAILED immediately)
 * - Any other exception → retryable
 *
 * Users can override with @RetryOn, @FailOn, @RecoverOn on class or method.
 */
@Slf4j
public class StepErrorHandler {

    public static void handleError(StepHandler<?> handler, Throwable ex) {
        int httpStatus = extractHttpStatus(ex);
        String errorMessage = ex.getMessage() != null ? ex.getMessage() : "";

        // Get annotations — MethodStepAdapter resolves method→class inheritance
        RecoverOn[] recoveries;
        FailOn failOn;
        RetryOn retryOn;

        if (handler instanceof MethodStepAdapter<?> adapter) {
            recoveries = adapter.getRecoverOns();
            failOn = adapter.getFailOn();
            retryOn = adapter.getRetryOn();
        } else {
            Class<?> cls = handler.getClass();
            recoveries = cls.getAnnotationsByType(RecoverOn.class);
            failOn = cls.getAnnotation(FailOn.class);
            retryOn = cls.getAnnotation(RetryOn.class);
        }

        // 1. @RecoverOn — auto-recover
        for (RecoverOn recover : recoveries) {
            if (httpStatus == recover.httpStatus()) {
                if (recover.message().isEmpty() || errorMessage.contains(recover.message())) {
                    log.info("[Step:{}] RECOVERED on HTTP {} (action={})",
                            handler.getStepName(), httpStatus, recover.action());
                    return;
                }
            }
        }

        // 2. @FailOn — fail immediately (explicit)
        if (failOn != null) {
            if (httpStatus > 0 && contains(failOn.httpStatus(), httpStatus)) {
                throw new NonRetryableStepException(
                        "HTTP " + httpStatus + " on " + handler.getStepName() + ": " + errorMessage, ex);
            }
            for (Class<? extends Throwable> t : failOn.exceptions()) {
                if (t.isInstance(ex) || (ex.getCause() != null && t.isInstance(ex.getCause()))) {
                    throw new NonRetryableStepException(
                            handler.getStepName() + " failed: " + errorMessage, ex);
                }
            }
        }

        // 3. @RetryOn — retry via Kafka (explicit)
        if (retryOn != null) {
            if (httpStatus > 0 && contains(retryOn.httpStatus(), httpStatus)) {
                throw new RetryableStepException(
                        "HTTP " + httpStatus + " on " + handler.getStepName() + ": " + errorMessage, ex);
            }
            for (Class<? extends Throwable> t : retryOn.exceptions()) {
                if (t.isInstance(ex) || (ex.getCause() != null && t.isInstance(ex.getCause()))) {
                    throw new RetryableStepException(
                            handler.getStepName() + " failed: " + errorMessage, ex);
                }
            }
        }

        // 4. Propagate if already typed
        if (ex instanceof RetryableStepException rse) throw rse;
        if (ex instanceof NonRetryableStepException nrse) throw nrse;

        // 5. BUILT-IN DEFAULTS (when no annotations match)
        if (httpStatus > 0) {
            // 5xx or 429 → retryable
            if (httpStatus >= 500 || httpStatus == 429) {
                throw new RetryableStepException(
                        "HTTP " + httpStatus + " on " + handler.getStepName() + ": " + errorMessage, ex);
            }
            // 4xx (except 429) → non-retryable
            if (httpStatus >= 400) {
                throw new NonRetryableStepException(
                        "HTTP " + httpStatus + " on " + handler.getStepName() + ": " + errorMessage, ex);
            }
        }

        // 6. Non-HTTP exception with no annotation → retryable (safe default)
        throw new RetryableStepException(handler.getStepName() + " failed: " + errorMessage, ex);
    }

    private static int extractHttpStatus(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            // Check the entire class hierarchy for getStatusCode() — covers:
            // HttpClientErrorException, HttpServerErrorException, HttpStatusCodeException,
            // WebClientResponseException, and any subclass (e.g. UnprocessableEntity)
            try {
                var method = current.getClass().getMethod("getStatusCode");
                var statusCode = method.invoke(current);
                return (int) statusCode.getClass().getMethod("value").invoke(statusCode);
            } catch (NoSuchMethodException ignored) {
                // Not an HTTP exception — try cause
            } catch (Exception ignored) {}
            current = current.getCause();
        }
        return 0;
    }

    private static boolean contains(int[] arr, int value) {
        for (int v : arr) if (v == value) return true;
        return false;
    }
}
