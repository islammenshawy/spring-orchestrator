package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.FailOn;
import com.orchestrator.starter.annotation.RecoverAction;
import com.orchestrator.starter.annotation.RecoverOn;
import com.orchestrator.starter.annotation.RetryOn;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads @RetryOn, @RecoverOn, @FailOn annotations from a StepHandler class
 * and maps exceptions to the correct behavior (retry, recover, fail).
 *
 * Called by FlowOrchestrator after a step throws. The user's execute() method
 * doesn't need any try/catch — the annotations declare the error handling.
 *
 * Resolution order:
 * 1. RecoverOn — if the error matches, treat as success (skip)
 * 2. FailOn — if the error matches, fail immediately (no retries)
 * 3. RetryOn — if the error matches, route to Kafka retry topics
 * 4. Default — if no annotation matches, treat as retryable
 */
@Slf4j
public class StepErrorHandler {

    /**
     * Resolves an exception thrown by a step handler into the correct behavior.
     * Returns null if the step should be treated as recovered (skip to next step).
     * Throws RetryableStepException or NonRetryableStepException otherwise.
     */
    public static void handleError(StepHandler<?> handler, Throwable ex) {
        Class<?> handlerClass = handler.getClass();
        int httpStatus = extractHttpStatus(ex);
        String errorMessage = ex.getMessage() != null ? ex.getMessage() : "";

        // 1. Check @RecoverOn — auto-recover (treat as success)
        RecoverOn[] recoveries = handlerClass.getAnnotationsByType(RecoverOn.class);
        for (RecoverOn recover : recoveries) {
            if (httpStatus == recover.httpStatus()) {
                if (recover.message().isEmpty() || errorMessage.contains(recover.message())) {
                    log.info("[StepErrorHandler] RECOVER on HTTP {} for {} (action={}): {}",
                            httpStatus, handler.getStepName(), recover.action(), errorMessage);
                    return; // Null = recovered, treat as success
                }
            }
        }

        // 2. Check @FailOn — fail immediately
        FailOn failOn = handlerClass.getAnnotation(FailOn.class);
        if (failOn != null) {
            if (httpStatus > 0 && contains(failOn.httpStatus(), httpStatus)) {
                throw new NonRetryableStepException(
                        "HTTP " + httpStatus + " on step " + handler.getStepName() + ": " + errorMessage, ex);
            }
            for (Class<? extends Throwable> failType : failOn.exceptions()) {
                if (failType.isInstance(ex) || (ex.getCause() != null && failType.isInstance(ex.getCause()))) {
                    throw new NonRetryableStepException(
                            handler.getStepName() + " failed (non-retryable): " + errorMessage, ex);
                }
            }
        }

        // 3. Check @RetryOn — route to retry topics
        RetryOn retryOn = handlerClass.getAnnotation(RetryOn.class);
        if (retryOn != null) {
            if (httpStatus > 0 && contains(retryOn.httpStatus(), httpStatus)) {
                throw new RetryableStepException(
                        "HTTP " + httpStatus + " on step " + handler.getStepName() + ": " + errorMessage, ex);
            }
            for (Class<? extends Throwable> retryType : retryOn.exceptions()) {
                if (retryType.isInstance(ex) || (ex.getCause() != null && retryType.isInstance(ex.getCause()))) {
                    throw new RetryableStepException(
                            handler.getStepName() + " failed (retryable): " + errorMessage, ex);
                }
            }
        }

        // 4. Default: if already a RetryableStepException or NonRetryableStepException, propagate
        if (ex instanceof RetryableStepException rse) throw rse;
        if (ex instanceof NonRetryableStepException nrse) throw nrse;

        // 5. Fallback: treat as retryable
        throw new RetryableStepException(
                handler.getStepName() + " failed: " + errorMessage, ex);
    }

    private static int extractHttpStatus(Throwable ex) {
        // Walk the cause chain looking for a status code
        Throwable current = ex;
        while (current != null) {
            // WebClientResponseException (Spring WebFlux)
            if (current.getClass().getSimpleName().equals("WebClientResponseException")) {
                try {
                    var method = current.getClass().getMethod("getStatusCode");
                    var statusCode = method.invoke(current);
                    var valueMethod = statusCode.getClass().getMethod("value");
                    return (int) valueMethod.invoke(statusCode);
                } catch (Exception ignored) {}
            }
            // HttpClientErrorException / HttpServerErrorException (Spring RestTemplate)
            if (current.getClass().getSimpleName().contains("HttpStatusCodeException")) {
                try {
                    var method = current.getClass().getMethod("getStatusCode");
                    var statusCode = method.invoke(current);
                    var valueMethod = statusCode.getClass().getMethod("value");
                    return (int) valueMethod.invoke(statusCode);
                } catch (Exception ignored) {}
            }
            current = current.getCause();
        }
        return 0; // No HTTP status found
    }

    private static boolean contains(int[] arr, int value) {
        for (int v : arr) {
            if (v == value) return true;
        }
        return false;
    }
}
