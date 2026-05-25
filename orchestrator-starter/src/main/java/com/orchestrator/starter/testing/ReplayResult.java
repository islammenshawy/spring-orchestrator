package com.orchestrator.starter.testing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of replaying a flow's step execution log against current code.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReplayResult {

    private String flowId;
    private int stepsReplayed;
    @Builder.Default
    private List<StepMismatch> mismatches = new ArrayList<>();

    public boolean allStepsMatched() {
        return mismatches.isEmpty();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StepMismatch {
        private String stepName;
        private int attemptNumber;
        private String expectedOutcome;
        private String actualOutcome;
        private String details;
    }
}
