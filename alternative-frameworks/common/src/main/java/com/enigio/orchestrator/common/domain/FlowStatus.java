package com.enigio.orchestrator.common.domain;

public enum FlowStatus {
    PENDING,
    IN_PROGRESS,
    WAITING_RETRY,
    COMPLETED,
    FAILED,
    COMPENSATING
}
