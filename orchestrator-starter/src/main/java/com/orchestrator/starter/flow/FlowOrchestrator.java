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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
@RequiredArgsConstructor
public class FlowOrchestrator<F extends OrchestratorFlow> {

    private final OrchestratorFlowRepository<F> flowRepository;
    private final StepRegistry<F> stepRegistry;
    private final OutboxEventRepository outboxRepository;
    private final StepExecutionLogRepository stepLogRepository;
    private final ObjectMapper objectMapper;
    private final String commandTopic;

    public F startFlow(F flow) {
        flow.setCurrentStep(stepRegistry.getFirstStep());
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now());
        flow = flowRepository.save(flow);
        writeOutboxEvent(flow);
        return flow;
    }

    public void executeStep(String flowId) {
        F flow = flowRepository.findById(flowId)
                .orElseThrow(() -> new IllegalArgumentException("Flow not found: " + flowId));

        if (flow.getStatus() == FlowStatus.COMPLETED) return;

        String stepName = flow.getCurrentStep();
        StepHandler<F> handler = stepRegistry.getHandler(stepName);

        // Layer 2 idempotency
        if (handler.isAlreadyCompleted(flow)) {
            log.info("[Saga] Step {} already completed for flow {}, advancing", stepName, flowId);
            advanceToNextStep(flow);
            return;
        }

        flow.setStatus(FlowStatus.IN_PROGRESS);
        String flowBefore = serialize(flow);
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
                        flowBefore, serialize(flow), e.getMessage(), startedAt);
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
                flowBefore, serialize(flow), null, startedAt);

        advanceToNextStep(flow);
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
            flowRepository.save(flow);
            log.info("[Saga] Flow {} completed", flow.getId());
        } else {
            flow.setCurrentStep(nextStep);
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
            writeOutboxEvent(flow);
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

    private void writeOutboxEvent(F flow) {
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .correlationId(flow.getCorrelationId())
                    .stepName(flow.getCurrentStep())
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

    private String serialize(F flow) {
        try {
            return objectMapper.writeValueAsString(flow);
        } catch (Exception e) {
            return "{}";
        }
    }
}
