package com.orchestrator.starter.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Reply dedup gate: tracks which steps have published a reply.
 * Unique compound index on (flowId, stepName) ensures exactly-once reply per step.
 *
 * Stored in a SEPARATE collection from the flow entity so saveFlow() can't overwrite it.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "orchestrator_step_completions")
@CompoundIndex(name = "flow_step_unique", def = "{'flowId': 1, 'stepName': 1}", unique = true)
public class StepCompletion {

    @Id
    private String id;

    private String flowId;
    private String stepName;

    @Indexed(expireAfterSeconds = 604800) // 7-day TTL
    private Instant completedAt;

    public StepCompletion(String flowId, String stepName) {
        this.flowId = flowId;
        this.stepName = stepName;
        this.completedAt = Instant.now();
    }
}
