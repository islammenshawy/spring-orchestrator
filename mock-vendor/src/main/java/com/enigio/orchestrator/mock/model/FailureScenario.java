package com.enigio.orchestrator.mock.model;

public enum FailureScenario {
    NONE,
    TIMEOUT,
    HTTP_500,
    HTTP_429,
    FLAKY
}
