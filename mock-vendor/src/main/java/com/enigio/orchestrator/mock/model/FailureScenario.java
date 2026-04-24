package com.enigio.orchestrator.mock.model;

public enum FailureScenario {
    NONE,
    TIMEOUT,
    HTTP_400,
    HTTP_403,
    HTTP_409,
    HTTP_429,
    HTTP_500,
    HTTP_502,
    HTTP_503,
    FLAKY
}
