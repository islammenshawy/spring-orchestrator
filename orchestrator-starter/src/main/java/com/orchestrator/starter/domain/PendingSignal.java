package com.orchestrator.starter.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * A signal queued for execution between steps.
 * When a signal arrives while a step is IN_PROGRESS, it's stored here
 * and processed after the step completes.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PendingSignal {
    private String signalName;
    private Map<String, Object> payload;
    private Instant queuedAt;
}
