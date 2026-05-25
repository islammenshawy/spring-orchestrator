package com.orchestrator.starter.testing;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.StepOutcome;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Set;

/**
 * Replays a flow's recorded step execution log against current step handlers.
 * Detects versioning bugs by comparing expected vs actual step outcomes.
 *
 * <pre>
 * @Test
 * void replayProduction() {
 *     var runner = new FlowReplayTestRunner<>(stepRegistry, logRepo, mapper, MyFlow.class);
 *     ReplayResult result = runner.replay("flow-abc-123");
 *     assertThat(result.allStepsMatched()).isTrue();
 * }
 * </pre>
 *
 * Requires: audit.include-flow-state=true for flow snapshots in step logs.
 */
@Slf4j
public class FlowReplayTestRunner<F extends OrchestratorFlow> {

    private final StepRegistry<F> stepRegistry;
    private final StepExecutionLogRepository logRepository;
    private final ObjectMapper objectMapper;
    private final Class<F> entityClass;

    /** Outcomes that indicate the step was actually executed (not skipped). */
    private static final Set<String> EXECUTABLE_OUTCOMES = Set.of(
            StepOutcome.COMPLETED.name(),
            StepOutcome.PARKED.name(),
            StepOutcome.WAITING.name(),
            StepOutcome.FAILED.name(),
            StepOutcome.RETRYING.name()
    );

    public FlowReplayTestRunner(StepRegistry<F> stepRegistry,
                                 StepExecutionLogRepository logRepository,
                                 ObjectMapper objectMapper,
                                 Class<F> entityClass) {
        this.stepRegistry = stepRegistry;
        this.logRepository = logRepository;
        this.objectMapper = objectMapper;
        this.entityClass = entityClass;
    }

    /**
     * Replay all logged step executions for a flow.
     * For each COMPLETED step: deserialize flow state, execute handler, compare outcome.
     */
    public ReplayResult replay(String flowId) {
        List<StepExecutionLog> logs = logRepository.findByFlowIdOrderByStartedAtAsc(flowId);

        var result = ReplayResult.builder()
                .flowId(flowId)
                .build();

        int replayed = 0;
        for (StepExecutionLog entry : logs) {
            // Only replay entries that were actually executed
            if (!EXECUTABLE_OUTCOMES.contains(entry.getStatus())) continue;

            // Need flow state snapshot to replay
            if (entry.getFlowStateBefore() == null) {
                log.debug("[Replay] Skipping {} — no flowStateBefore snapshot", entry.getStepName());
                continue;
            }

            try {
                F flow = objectMapper.readValue(entry.getFlowStateBefore(), entityClass);
                StepHandler<F> handler = stepRegistry.getHandler(entry.getStepName());

                String actualOutcome;
                try {
                    handler.execute(flow);
                    actualOutcome = StepOutcome.COMPLETED.name();
                } catch (WaitingStepException e) {
                    actualOutcome = e.isParked() ? StepOutcome.PARKED.name() : StepOutcome.WAITING.name();
                } catch (com.orchestrator.starter.exception.RetryableStepException e) {
                    actualOutcome = StepOutcome.RETRYING.name();
                } catch (com.orchestrator.starter.exception.NonRetryableStepException e) {
                    actualOutcome = StepOutcome.FAILED.name();
                } catch (Exception e) {
                    actualOutcome = StepOutcome.RETRYING.name(); // infrastructure error → retryable
                }

                String expectedOutcome = entry.getStatus();
                if (!expectedOutcome.equals(actualOutcome)) {
                    result.getMismatches().add(ReplayResult.StepMismatch.builder()
                            .stepName(entry.getStepName())
                            .attemptNumber(entry.getAttemptNumber())
                            .expectedOutcome(expectedOutcome)
                            .actualOutcome(actualOutcome)
                            .details("Step outcome changed from " + expectedOutcome + " to " + actualOutcome)
                            .build());
                    log.warn("[Replay] MISMATCH at {} attempt {}: expected={}, actual={}",
                            entry.getStepName(), entry.getAttemptNumber(), expectedOutcome, actualOutcome);
                }
                replayed++;

            } catch (Exception e) {
                log.warn("[Replay] Failed to replay {} for flow {}: {}",
                        entry.getStepName(), flowId, e.getMessage());
                result.getMismatches().add(ReplayResult.StepMismatch.builder()
                        .stepName(entry.getStepName())
                        .attemptNumber(entry.getAttemptNumber())
                        .expectedOutcome(entry.getStatus())
                        .actualOutcome("ERROR")
                        .details("Replay failed: " + e.getMessage())
                        .build());
            }
        }

        result.setStepsReplayed(replayed);
        log.info("[Replay] Flow {} — {} steps replayed, {} mismatches",
                flowId, replayed, result.getMismatches().size());
        return result;
    }

    /** Replay multiple flows. */
    public List<ReplayResult> replayFlows(List<String> flowIds) {
        return flowIds.stream().map(this::replay).toList();
    }
}
