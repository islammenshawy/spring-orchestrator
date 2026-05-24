package com.orchestrator.starter.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * A signal queued for execution between steps.
 * When a signal arrives while a step is IN_PROGRESS, it's stored here
 * and processed after the step completes.
 *
 * Payload is stored as serialized JSON (since arbitrary Objects can't
 * be stored directly in MongoDB). Deserialized to the handler's typed
 * parameter when drained.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PendingSignal {
    private String signalName;
    private String payloadJson;
    private Instant queuedAt;
}
