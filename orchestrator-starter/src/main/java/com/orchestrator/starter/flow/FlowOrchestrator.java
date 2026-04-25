package com.orchestrator.starter.flow;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.kafka.StepCommandMessage;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Saga orchestrator with transactional outbox.
 *
 * Executes steps one-per-Kafka-message, persists state + outbox atomically,
 * logs every step attempt, and runs compensation in reverse on permanent failure.
 */
@Slf4j
public class FlowOrchestrator<F extends OrchestratorFlow> {

    private final OrchestratorFlowRepository<F> flowRepository;
    private final StepRegistry<F> stepRegistry;
    private final OutboxEventRepository outboxRepository;
    private final StepExecutionLogRepository stepLogRepository;
    private final ObjectMapper objectMapper;
    private final String commandTopic;
    private final TransactionTemplate txTemplate;
    private final boolean includeFlowStateInLogs;

    public FlowOrchestrator(
            OrchestratorFlowRepository<F> flowRepository,
            StepRegistry<F> stepRegistry,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper,
            String commandTopic,
            TransactionTemplate txTemplate,
            boolean includeFlowStateInLogs) {
        this.flowRepository = flowRepository;
        this.stepRegistry = stepRegistry;
        this.outboxRepository = outboxRepository;
        this.stepLogRepository = stepLogRepository;
        this.objectMapper = objectMapper;
        this.commandTopic = commandTopic;
        this.includeFlowStateInLogs = includeFlowStateInLogs;
        this.txTemplate = txTemplate;
    }

    public F startFlow(F flow) {
        flow.setCurrentStep(stepRegistry.getFirstStep());
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now());

        // Atomic: flow save + outbox event in one transaction (if available)
        F savedFlow = runInTransaction(() -> {
            F f = flowRepository.save(flow);
            writeOutboxEvent(f);
            return f;
        }, flow);

        return savedFlow;
    }

    /**
     * Execute a step by name. For sequential steps, stepName matches currentStep.
     * For parallel steps, stepName is the specific parallel step from the Kafka message.
     */
    public void executeStep(String flowId, String stepName) {
        F flow = flowRepository.findById(flowId)
                .orElseThrow(() -> new IllegalArgumentException("Flow not found: " + flowId));

        if (flow.getStatus() == FlowStatus.COMPLETED) return;

        // Use the step name from the Kafka message (supports parallel steps)
        if (stepName == null) stepName = flow.getCurrentStep();
        StepHandler<F> handler = stepRegistry.getHandler(stepName);

        // Check if this is a join point — all parallel steps must be done first
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isJoinPoint()) {
            String group = adapter.getJoinOnGroup();
            List<StepHandler<F>> parallelSteps = stepRegistry.getParallelGroup(group);
            boolean allDone = parallelSteps.stream()
                    .allMatch(ps -> ps.isAlreadyCompleted(flow));
            if (!allDone) {
                log.info("[Saga] Join {} waiting — not all parallel steps in group '{}' completed",
                        stepName, group);
                return; // Don't ack — message will be redelivered
            }
            log.info("[Saga] Join {} — all parallel steps in group '{}' completed, proceeding",
                    stepName, group);
        }

        // Layer 2 idempotency
        if (handler.isAlreadyCompleted(flow)) {
            log.info("[Saga] Step {} already completed for flow {}, marking parallel + advancing",
                    stepName, flowId);
            markParallelStepCompleted(flow, stepName, handler);
            return;
        }

        flow.setStatus(FlowStatus.IN_PROGRESS);
        String flowBefore = includeFlowStateInLogs ? serialize(flow) : null;
        Instant startedAt = Instant.now();

        log.info("[Saga] Executing step {} for flow {}", stepName, flowId);

        try {
            handler.execute(flow);
        } catch (RetryableStepException e) {
            logStep(flowId, stepName, "RETRYING", flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handleRetryableFailure(flow, e);
            throw e;
        } catch (NonRetryableStepException e) {
            logStep(flowId, stepName, "FAILED", flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handlePermanentFailure(flow, e);
            return;
        } catch (Exception e) {
            try {
                StepErrorHandler.handleError(handler, e);
                // Recovered (e.g., HTTP 409)
                logStep(flowId, stepName, "RECOVERED", flow.getRetryCount() + 1,
                        flowBefore, includeFlowStateInLogs ? serialize(flow) : null, e.getMessage(), startedAt);
                log.info("[Saga] Step {} recovered for flow {}", stepName, flowId);
            } catch (RetryableStepException re) {
                logStep(flowId, stepName, "RETRYING", flow.getRetryCount() + 1,
                        flowBefore, null, re.getMessage(), startedAt);
                handleRetryableFailure(flow, re);
                throw re;
            } catch (NonRetryableStepException nre) {
                logStep(flowId, stepName, "FAILED", flow.getRetryCount() + 1,
                        flowBefore, null, nre.getMessage(), startedAt);
                handlePermanentFailure(flow, nre);
                return;
            }
        }

        // Step succeeded
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);

        logStep(flowId, stepName, "COMPLETED", 1,
                flowBefore, includeFlowStateInLogs ? serialize(flow) : null, null, startedAt);

        markParallelStepCompleted(flow, stepName, handler);
    }

    public void markDeadLettered(String flowId) {
        flowRepository.findById(flowId).ifPresent(flow -> {
            flow.setStatus(FlowStatus.FAILED);
            flow.setErrorMessage("[DLT] Exhausted all retry attempts");
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);

            logStep(flowId, flow.getCurrentStep(), "DEAD_LETTERED", flow.getRetryCount(),
                    null, null, "[DLT] Exhausted retries", Instant.now());

            // Run compensation for all completed steps in reverse
            runCompensation(flow);
        });
    }

    // ========== Compensation (what makes this a Saga) ==========

    private void runCompensation(F flow) {
        List<String> completedSteps = stepRegistry.getCompletedStepsBefore(flow.getCurrentStep());
        if (completedSteps.isEmpty()) return;

        log.info("[Saga] Running compensation for flow {} — {} steps to undo",
                flow.getId(), completedSteps.size());

        flow.setStatus(FlowStatus.COMPENSATING);
        flowRepository.save(flow);

        // Compensate in reverse order
        for (int i = completedSteps.size() - 1; i >= 0; i--) {
            String stepName = completedSteps.get(i);
            StepHandler<F> handler = stepRegistry.getHandler(stepName);

            if (handler instanceof MethodStepAdapter<F> adapter && adapter.hasCompensation()) {
                Instant start = Instant.now();
                try {
                    adapter.compensate(flow);
                    logStep(flow.getId(), stepName, "COMPENSATED", 1, null, null, null, start);
                } catch (Exception e) {
                    log.error("[Saga] Compensation failed for step {} on flow {}: {}",
                            stepName, flow.getId(), e.getMessage());
                    logStep(flow.getId(), stepName, "COMPENSATION_FAILED", 1,
                            null, null, e.getMessage(), start);
                }
            } else {
                log.warn("[Saga] No @Compensate for step {}, skipping", stepName);
            }
        }

        flow.setStatus(FlowStatus.FAILED);
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);
    }

    // ========== Internal ==========

    private void advanceToNextStep(F flow) {
        String nextStep = stepRegistry.getNextStep(flow.getCurrentStep());
        if (nextStep == null) {
            flow.setStatus(FlowStatus.COMPLETED);
            flow.setUpdatedAt(Instant.now());
            flow.setCompletedParallelSteps(new java.util.HashSet<>());
            flowRepository.save(flow);
            log.info("[Saga] Flow {} completed", flow.getId());
            return;
        }

        List<String> stepsAtNextOrder = stepRegistry.getStepsAtSameOrder(nextStep);
        if (stepsAtNextOrder.size() > 1) {
            // Parallel: atomic flow save + multiple outbox events
            flow.setCurrentStep(nextStep);
            flow.setCompletedParallelSteps(new java.util.HashSet<>());
            flow.setUpdatedAt(Instant.now());
            runInTransaction(() -> {
                flowRepository.save(flow);
                for (String parallelStep : stepsAtNextOrder) {
                    writeOutboxEvent(flow, parallelStep);
                }
                return null;
            }, null);
            log.info("[Saga] Published {} parallel steps for flow {}: {}",
                    stepsAtNextOrder.size(), flow.getId(), stepsAtNextOrder);
        } else {
            // Sequential: atomic flow save + outbox event
            flow.setCurrentStep(nextStep);
            flow.setUpdatedAt(Instant.now());
            runInTransaction(() -> {
                flowRepository.save(flow);
                writeOutboxEvent(flow);
                return null;
            }, null);
        }
    }

    private void handleRetryableFailure(F flow, RetryableStepException e) {
        flow.setRetryCount(flow.getRetryCount() + 1);
        int backoff = (int) Math.min(Math.pow(2, flow.getRetryCount()), 60);
        flow.setBackoffSeconds(backoff);
        flow.setNextRetryAt(Instant.now().plusSeconds(backoff));
        flow.setStatus(FlowStatus.WAITING_RETRY);
        flow.setErrorMessage(e.getMessage());
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);
    }

    private void handlePermanentFailure(F flow, NonRetryableStepException e) {
        flow.setStatus(FlowStatus.FAILED);
        flow.setErrorMessage(e.getMessage());
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);

        // Saga compensation — undo completed steps in reverse
        runCompensation(flow);
    }

    /**
     * After a parallel step completes, track it and check if all siblings are done.
     * If all done, advance to the next step (which may be a @JoinOn step).
     */
    private void markParallelStepCompleted(F flow, String stepName, StepHandler<F> handler) {
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isParallel()) {
            java.util.Set<String> completed = new java.util.HashSet<>(flow.getCompletedParallelSteps());
            completed.add(stepName);
            flow.setCompletedParallelSteps(completed);
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);

            // Check if all parallel siblings are done
            List<StepHandler<F>> siblings = stepRegistry.getParallelGroup(adapter.getParallelGroup());
            boolean allDone = siblings.stream().allMatch(s -> s.isAlreadyCompleted(flow));

            if (allDone) {
                log.info("[Saga] All parallel steps in group '{}' completed for flow {}",
                        adapter.getParallelGroup(), flow.getId());
                advanceToNextStep(flow);
            } else {
                log.info("[Saga] Parallel step {} done, waiting for siblings in group '{}'",
                        stepName, adapter.getParallelGroup());
            }
        } else {
            // Sequential step — just advance
            advanceToNextStep(flow);
        }
    }

    private void writeOutboxEvent(F flow) {
        writeOutboxEvent(flow, flow.getCurrentStep());
    }

    private void writeOutboxEvent(F flow, String stepNameOverride) {
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .correlationId(flow.getCorrelationId())
                    .stepName(stepNameOverride)
                    .build();

            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .topic(commandTopic)
                    .key(flow.getId())
                    .payload(objectMapper.writeValueAsString(cmd))
                    .build();

            outboxRepository.save(event);
        } catch (Exception e) {
            log.error("[Saga] Failed to write outbox event: {}", e.getMessage());
        }
    }

    private void logStep(String flowId, String stepName, String status, int attempt,
                         String before, String after, String error, Instant startedAt) {
        try {
            Instant now = Instant.now();
            stepLogRepository.save(StepExecutionLog.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .stepName(stepName)
                    .status(status)
                    .attemptNumber(attempt)
                    .flowStateBefore(before)
                    .flowStateAfter(after)
                    .errorMessage(error)
                    .durationMs(now.toEpochMilli() - startedAt.toEpochMilli())
                    .startedAt(startedAt)
                    .completedAt(now)
                    .build());
        } catch (Exception e) {
            log.warn("[Saga] Failed to log step execution: {}", e.getMessage());
        }
    }

    /**
     * Runs the given action in a MongoDB transaction if available.
     * On standalone MongoDB (no TransactionTemplate): runs without transaction.
     * On replica set with transactions enabled: fully atomic.
     *
     * The user never touches this — the library handles it internally
     * for all flow save + outbox event writes.
     */
    private <R> R runInTransaction(java.util.function.Supplier<R> action, R fallback) {
        if (txTemplate != null) {
            return txTemplate.execute(status -> action.get());
        }
        return action.get();
    }

    private String serialize(F flow) {
        try {
            return objectMapper.writeValueAsString(flow);
        } catch (Exception e) {
            return "{}";
        }
    }
}
