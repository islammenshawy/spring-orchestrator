package com.orchestrator.starter.domain;

public enum FlowStatus {
    PENDING,
    IN_PROGRESS,
    WAITING_RETRY,
    COMPLETED,
    COMPENSATING,
    FAILED,
    CANCELLING,
    CANCELLED
}
