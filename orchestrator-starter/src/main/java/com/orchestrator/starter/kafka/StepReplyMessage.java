package com.orchestrator.starter.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Published to the reply topic after a step completes (success or failure).
 * The orchestrator reply consumer picks this up and decides the next action.
 *
 * This decouples step execution from orchestration:
 * - Executor thread: execute step → publish reply (fast, non-blocking)
 * - Orchestrator thread: consume reply → advance flow → publish next command
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepReplyMessage {

    private String flowId;
    private String stepName;
    private String eventId;
    private String status;       // COMPLETED, FAILED, RECOVERED
    private String errorMessage;  // null on success
    private String flowType;

    /** Serialized flow state after step execution.
     *  The reply consumer deserializes this instead of re-reading from MongoDB.
     *  Eliminates race condition between command consumer save and reply consumer read. */
    private String flowSnapshot;
}
