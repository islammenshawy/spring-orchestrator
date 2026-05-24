package com.orchestrator.starter.domain;

public enum FlowStatus {
    PENDING,
    IN_PROGRESS,
    WAITING_RETRY,
    PARKED,
    COMPLETED,
    COMPENSATING,
    COMPENSATION_FAILED,
    FAILED,
    CANCELLING,
    CANCELLED
}
