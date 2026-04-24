package com.orchestrator.starter.annotation;

/**
 * Declares what kind of operation a step performs.
 * The library applies different levels of protection based on type.
 */
public enum StepType {

    /**
     * External API call (default). Full protection:
     * - Kafka retry topics with jittered backoff
     * - @RetryOn / @RecoverOn / @FailOn annotation handling
     * - Idempotency guard via completedWhen
     * - Outbox for next-step command
     */
    API_CALL,

    /**
     * Database write (internal). Minimal protection:
     * - No Kafka retry topics (MongoDB handles atomicity)
     * - Executed inline, not via Kafka message
     * - If it fails, the enclosing API_CALL step retries from the beginning
     */
    DB_WRITE,

    /**
     * Database read or internal query. No protection needed:
     * - Side-effect free, safe to repeat
     * - No idempotency check
     * - No retry
     */
    QUERY
}
