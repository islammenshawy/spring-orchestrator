package com.orchestrator.starter.exception;

import java.time.Duration;

/**
 * Thrown internally by {@code waitUntil()} to signal that a step is waiting.
 *
 * Two modes:
 * <ul>
 *   <li><b>PARKED</b> — gate step waiting for an external trigger (webhook, API).
 *       Flow sleeps in MongoDB until explicitly re-activated. No Kafka cycling.</li>
 *   <li><b>POLLING</b> — step actively polls for a condition on a configured interval.
 *       Flow is re-delivered via scheduler when {@code nextRetryAt} elapses.</li>
 * </ul>
 *
 * Users should not throw this directly — use {@code waitUntil()} overloads
 * on {@link com.orchestrator.starter.flow.FlowDefinition}.
 */
public class WaitingStepException extends RuntimeException {

    /** Determines how the orchestrator handles a waiting step. */
    public enum WaitMode { PARKED, POLLING }

    private final WaitMode waitMode;
    private final Duration pollInterval;
    private final Duration expiry;

    /**
     * Full constructor used by {@code FlowDefinition.waitUntil()} and {@code pollUntil()}.
     *
     * @param message       descriptive message for logs
     * @param waitMode      PARKED (gate) or POLLING (active re-check)
     * @param pollInterval  re-check interval (required for POLLING, null for PARKED)
     * @param expiry        max time to wait before failing the flow
     */
    public WaitingStepException(String message, WaitMode waitMode, Duration pollInterval, Duration expiry) {
        super(message);
        this.waitMode = waitMode;
        this.pollInterval = pollInterval;
        this.expiry = expiry;
    }

    public WaitMode getWaitMode() { return waitMode; }
    public Duration getPollInterval() { return pollInterval; }
    public Duration getExpiry() { return expiry; }
    public boolean isParked() { return waitMode == WaitMode.PARKED; }
}
