package com.orchestrator.starter.failover;

/**
 * DC health state machine states.
 *
 * HEALTHY → DEGRADED → FAILING_OVER → COOLDOWN → HEALTHY
 *                ↑                                   |
 *                └───────────────────────────────────┘
 */
public enum DcState {
    /** Active DC is healthy — probes passing. */
    HEALTHY,
    /** Probes failing but below failover threshold — alerting, no action yet. */
    DEGRADED,
    /** Failover in progress — stopping active, starting standby. */
    FAILING_OVER,
    /** Failover complete — in dwell period before allowing another transition. */
    COOLDOWN
}
