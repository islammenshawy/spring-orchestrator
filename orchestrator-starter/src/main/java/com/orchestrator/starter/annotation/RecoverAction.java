package com.orchestrator.starter.annotation;

/**
 * What to do when a recoverable condition is detected.
 */
public enum RecoverAction {

    /** Treat as success — skip the API call, advance to next step */
    SKIP,

    /** Treat as success and extract the existing resource ID from the error response */
    SKIP_AND_EXTRACT
}
