package com.orchestrator.starter.exception;

/**
 * Throw from a step handler to signal a retryable failure.
 * The orchestrator will route the message to Kafka retry topics
 * with exponential backoff + jitter.
 */
public class RetryableStepException extends RuntimeException {

    public RetryableStepException(String message) {
        super(message);
    }

    public RetryableStepException(String message, Throwable cause) {
        super(message, cause);
    }
}
