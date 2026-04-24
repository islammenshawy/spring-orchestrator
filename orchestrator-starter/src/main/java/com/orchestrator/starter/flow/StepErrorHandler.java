package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.FailOn;
import com.orchestrator.starter.annotation.RecoverOn;
import com.orchestrator.starter.annotation.RetryOn;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads @RetryOn, @RecoverOn, @FailOn annotations from a step handler
 * and maps exceptions to the correct behavior.
 *
 * For MethodStepAdapter: reads from method first, falls back to class level.
 * For standalone StepHandler: reads from the class directly.
 *
 * Resolution order:
 * 1. @RecoverOn match → return (treat as success, skip to next step)
 * 2. @FailOn match → throw NonRetryableStepException
 * 3. @RetryOn match → throw RetryableStepException
 * 4. Default → treat as retryable
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

        // 2. @FailOn — fail immediately
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

        // 3. @RetryOn — retry via Kafka
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

        // 5. Default: retryable
        throw new RetryableStepException(handler.getStepName() + " failed: " + errorMessage, ex);
    }

    private static int extractHttpStatus(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            String className = current.getClass().getSimpleName();
            if (className.equals("WebClientResponseException") ||
                    className.contains("HttpStatusCodeException")) {
                try {
                    var statusCode = current.getClass().getMethod("getStatusCode").invoke(current);
                    return (int) statusCode.getClass().getMethod("value").invoke(statusCode);
                } catch (Exception ignored) {}
            }
            current = current.getCause();
        }
        return 0;
    }

    private static boolean contains(int[] arr, int value) {
        for (int v : arr) if (v == value) return true;
        return false;
    }
}
