package com.orchestrator.starter.exception;

/**
 * Throw from a step handler to signal a permanent failure.
 * The flow will be marked FAILED immediately — no retries.
 */
public class NonRetryableStepException extends RuntimeException {

    public NonRetryableStepException(String message) {
        super(message);
    }

    public NonRetryableStepException(String message, Throwable cause) {
        super(message, cause);
    }
}
