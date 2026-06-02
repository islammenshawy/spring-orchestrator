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

    /**
     * Extract HTTP status code from any exception in the cause chain.
     * Uses reflection to avoid compile-time dependencies on Spring Web, WebFlux, or Feign.
     *
     * Supported exception types:
     * - Spring RestClient/WebClient: getStatusCode().value() → int
     * - Feign: status() → int
     */
    private static int extractHttpStatus(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            int status = tryExtractSpringStatus(current);
            if (status > 0) return status;

            status = tryExtractFeignStatus(current);
            if (status > 0) return status;

            current = current.getCause();
        }
        return 0;
    }

    /** Spring HTTP: HttpClientErrorException, HttpServerErrorException, WebClientResponseException */
    private static int tryExtractSpringStatus(Throwable ex) {
        try {
            var statusCode = ex.getClass().getMethod("getStatusCode").invoke(ex);
            return (int) statusCode.getClass().getMethod("value").invoke(statusCode);
        } catch (NoSuchMethodException e) {
            return 0;
        } catch (Exception e) {
            log.trace("Failed to extract Spring HTTP status from {}: {}", ex.getClass().getSimpleName(), e.getMessage());
            return 0;
        }
    }

    /** Feign: FeignException.status() returns int */
    private static int tryExtractFeignStatus(Throwable ex) {
        try {
            var method = ex.getClass().getMethod("status");
            if (method.getReturnType() == int.class) {
                return (int) method.invoke(ex);
            }
        } catch (NoSuchMethodException e) {
            return 0;
        } catch (Exception e) {
            log.trace("Failed to extract Feign HTTP status from {}: {}", ex.getClass().getSimpleName(), e.getMessage());
        }
        return 0;
    }

    private static boolean contains(int[] arr, int value) {
        for (int v : arr) if (v == value) return true;
        return false;
    }
}
