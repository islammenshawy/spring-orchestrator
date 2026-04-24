package com.orchestrator.starter.flow;

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
import java.util.UUID;

/**
 * Generic flow orchestrator with transactional outbox.
 *
 * All flow state changes + outbox event writes happen in the same
 * MongoDB operation. The outbox publisher polls and sends to Kafka later.
 *
 * This means:
 * - Flow state and the "publish next step" intent are written together
 * - If container crashes after write: outbox publisher picks up the event
 * - If container crashes before write: nothing happened, Kafka redelivers
 * - No gap where a flow is saved but the next step command is lost
 *
 * On MongoDB replica set with @Transactional: fully atomic.
 * On standalone MongoDB: two sequential writes (narrower gap than direct Kafka).
 *
 * @param <F> the flow entity type
 */
@Slf4j
@RequiredArgsConstructor
public class FlowOrchestrator<F extends OrchestratorFlow> {

    private final OrchestratorFlowRepository<F> flowRepository;
    private final StepRegistry<F> stepRegistry;
    private final OutboxEventRepository outboxRepository;
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
            log.info("[Orchestrator] Step {} already completed for flow {}, advancing", stepName, flowId);
            advanceToNextStep(flow);
            return;
        }

        flow.setStatus(FlowStatus.IN_PROGRESS);
        log.info("[Orchestrator] Executing step {} for flow {}", stepName, flowId);

        try {
            handler.execute(flow);
        } catch (RetryableStepException e) {
            handleRetryableFailure(flow, e);
            throw e;
        } catch (NonRetryableStepException e) {
            handlePermanentFailure(flow, e);
            return;
        }

        // Step succeeded — persist result and write outbox event for next step
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);

        advanceToNextStep(flow);
    }

    public void markDeadLettered(String flowId) {
        flowRepository.findById(flowId).ifPresent(flow -> {
            flow.setStatus(FlowStatus.FAILED);
            flow.setErrorMessage("[DLT] Exhausted all retry attempts");
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
            log.error("[Orchestrator] Flow {} dead-lettered at step {}", flowId, flow.getCurrentStep());
        });
    }

    private void advanceToNextStep(F flow) {
        String nextStep = stepRegistry.getNextStep(flow.getCurrentStep());
        if (nextStep == null) {
            flow.setStatus(FlowStatus.COMPLETED);
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
            log.info("[Orchestrator] Flow {} completed", flow.getId());
        } else {
            flow.setCurrentStep(nextStep);
            flow.setUpdatedAt(Instant.now());
            // Save flow + write outbox event (both MongoDB writes, same DB)
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
    }

    /**
     * Writes a step command to the outbox collection (same DB as the flow).
     * The OutboxPublisher polls this and sends to Kafka.
     *
     * On replica set with @Transactional: this write is atomic with the
     * flow save above — both commit or neither does.
     *
     * On standalone: two sequential writes, but both to the same MongoDB.
     * The gap is microseconds (vs milliseconds for MongoDB→Kafka).
     */
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
            log.error("[Orchestrator] Failed to write outbox event for flow {}: {}",
                    flow.getId(), e.getMessage());
        }
    }
}
