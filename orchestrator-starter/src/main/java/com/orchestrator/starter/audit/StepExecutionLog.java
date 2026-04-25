package com.orchestrator.starter.audit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Audit trail for every step execution attempt.
 * Automatically created by the library — users don't interact with this.
 *
 * Records: which step, which attempt, what was the flow state before/after,
 * how long it took, what error occurred (if any).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "orchestrator_step_log")
@CompoundIndex(name = "flow_step_idx", def = "{'flowId': 1, 'stepName': 1, 'attemptNumber': 1}")
@CompoundIndex(name = "flow_started_idx", def = "{'flowId': 1, 'startedAt': 1}")
public class StepExecutionLog {

    @Id
    private String id;

    private String flowId;
    private String stepName;
    private String status;      // EXECUTING, COMPLETED, FAILED, RECOVERED, COMPENSATED
    private int attemptNumber;

    private String flowStateBefore;  // JSON snapshot of flow before step
    private String flowStateAfter;   // JSON snapshot of flow after step
    private String errorMessage;

    private long durationMs;
    private Instant startedAt;
    private Instant completedAt;
}
