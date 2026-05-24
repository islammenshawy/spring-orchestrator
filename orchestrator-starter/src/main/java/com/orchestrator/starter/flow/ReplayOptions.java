package com.orchestrator.starter.flow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Options for replaying a flow.
 *
 * <pre>
 * orchestrator.replayFlow(flowId, ReplayOptions.builder()
 *     .fromStep("CREATE_DRAFT")
 *     .allowCompleted(true)
 *     .build());
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReplayOptions {

    /** Restart from this step. Null = resume from current (failed) step. */
    private String fromStep;

    /** Allow replaying COMPLETED flows. Default false — must opt in explicitly. */
    @Builder.Default
    private boolean allowCompleted = false;
}
