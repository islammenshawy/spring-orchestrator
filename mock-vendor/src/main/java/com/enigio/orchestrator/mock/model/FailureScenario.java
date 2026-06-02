package com.enigio.orchestrator.mock.model;

public enum FailureScenario {
    NONE,
    TIMEOUT,
    HTTP_400,
    HTTP_401,
    HTTP_403,
    HTTP_404,
    HTTP_409,
    HTTP_422,
    HTTP_429,
    HTTP_500,
    HTTP_502,
    HTTP_503,
    HTTP_504,
    FLAKY
}
