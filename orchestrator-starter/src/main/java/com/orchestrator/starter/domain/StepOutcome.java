package com.orchestrator.starter.domain;

/**
 * Outcome of a step execution attempt. Used in audit logs
 * and metrics instead of string literals.
 */
public enum StepOutcome {
    COMPLETED, WAITING, PARKED, RETRYING, FAILED, RECOVERED,
    COMPENSATED, COMPENSATION_FAILED, CANCELLED, CANCEL_FAILED, DEAD_LETTERED
}
