package com.orchestrator.starter.exception;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Throw from a gate/polling step to signal "waiting for an external event."
 *
 * Unlike {@link RetryableStepException} (vendor errors → exponential backoff),
 * this uses a <b>fixed short interval with jitter</b> for fast re-polling.
 * The orchestrator re-publishes to the main command topic directly,
 * bypassing Spring Kafka's exponential retry topics.
 *
 * Use cases:
 * - Waiting for downstream approval (gate steps)
 * - Polling for signing completion
 * - Waiting for webhook-driven state changes
 *
 * Default: 5s base + 50% jitter = 2.5-5s actual delay.
 */
public class WaitingStepException extends RuntimeException {

    private final long delayMs;

    /** Default: 5s base + 50% jitter. */
    public WaitingStepException(String message) {
        this(message, 5000, 0.5);
    }

    /** Custom base delay + jitter. */
    public WaitingStepException(String message, long baseDelayMs, double jitterFactor) {
        super(message);
        this.delayMs = applyJitter(baseDelayMs, jitterFactor);
    }

    /** The computed delay (with jitter already applied). */
    public long getDelayMs() {
        return delayMs;
    }

    public Duration getDelay() {
        return Duration.ofMillis(delayMs);
    }

    private static long applyJitter(long base, double jitterFactor) {
        long jitterRange = (long) (base * jitterFactor);
        long fixed = base - jitterRange;
        long jitter = jitterRange > 0 ? ThreadLocalRandom.current().nextLong(0, jitterRange + 1) : 0;
        return fixed + jitter;
    }
}
