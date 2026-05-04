package com.enigio.orchestrator.saga.saga;

import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.common.domain.FlowStep;
import com.enigio.orchestrator.common.exception.NonRetryableException;
import com.enigio.orchestrator.common.exception.RetryableException;
import com.enigio.orchestrator.saga.outbox.OutboxService;
import com.enigio.orchestrator.saga.saga.steps.SagaStep;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.enigio.orchestrator.common.kafka.StepResultMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final DocumentFlowRepository flowRepository;
    private final SagaStepRegistry stepRegistry;
    private final SagaStepLogRepository stepLogRepository;
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Value("${enigio.client.max-retries:3}")
    private int maxRetries;

    /**
     * Starts a flow. Atomically sets status to IN_PROGRESS and writes the
     * first step command to the outbox. If the container crashes after this
     * transaction commits, the outbox publisher will pick up the event.
     * If it crashes before commit, neither the flow update nor the outbox
     * event exist — the flow stays PENDING and can be retried.
     */
    @Transactional
    public void startFlow(DocumentFlow flow) {
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setCurrentStep(FlowStep.CREATE_DOCUMENT);
        flow.setUpdatedAt(Instant.now());
        flowRepository.save(flow);

        publishStepCommand(flow, FlowStep.CREATE_DOCUMENT);
    }

    /**
     * Executes a step. The Enigio API call is NOT inside the transaction
     * (external calls shouldn't be in a transaction). The pattern is:
     *
     * 1. Call Enigio API (outside transaction)
     * 2. In a transaction: save flow state + step log + outbox event
     *
     * If container crashes during step 1: Kafka will redeliver the message,
     * idempotency check prevents duplicate processing if the step already completed.
     *
     * If container crashes during step 2 (transaction): everything rolls back,
     * Kafka redelivers, step re-executes (Enigio call is idempotent or safe to retry).
     *
     * On RetryableException: re-throws so Spring Kafka routes to retry topics.
     */
    public void executeStep(String flowId, FlowStep step) {
        DocumentFlow flow = flowRepository.findById(flowId)
                .orElseThrow(() -> new IllegalArgumentException("Flow not found: " + flowId));

        SagaStep sagaStep = stepRegistry.getStep(step);
        String requestPayload = serializeFlow(flow);

        // Step 1: Call Enigio (outside transaction — external call)
        try {
            flow = sagaStep.execute(flow);
        } catch (RetryableException e) {
            // Transactionally update flow state for the failure
            persistStepFailure(flow, step, requestPayload, e.getMessage(), true);
            throw e; // Re-throw for Spring Kafka retry topics
        } catch (NonRetryableException e) {
            persistStepFailure(flow, step, requestPayload, e.getMessage(), false);
            return; // Don't re-throw — no point retrying
        }

        // Step 2: Persist results atomically (inside transaction)
        persistStepSuccess(flow, step, requestPayload);
    }

    /**
     * Atomically: save flow state + step log (COMPLETED) + outbox event for next step.
     * If container crashes before this commits → Kafka redelivers → step re-executes.
     * If container crashes after commit → outbox publisher picks up the next step command.
     */
    @Transactional
    public void persistStepSuccess(DocumentFlow flow, FlowStep step, String requestPayload) {
        flow.setUpdatedAt(Instant.now());
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flowRepository.save(flow);

        SagaStepLog stepLog = SagaStepLog.builder()
                .id(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .stepName(step.name())
                .status("COMPLETED")
                .attemptNumber(flow.getRetryCount() + 1)
                .requestPayload(requestPayload)
                .responsePayload(serializeFlow(flow))
                .startedAt(Instant.now())
                .completedAt(Instant.now())
                .build();
        stepLogRepository.save(stepLog);

        publishStepResult(flow, step, true, null, false);
    }

    /**
     * Atomically: save flow state + step log (FAILED/RETRYING).
     * For retryable errors, the exception propagates to Spring Kafka
     * which routes to retry topics with exponential backoff.
     */
    @Transactional
    public void persistStepFailure(DocumentFlow flow, FlowStep step,
                                   String requestPayload, String errorMessage, boolean retryable) {
        flow.setRetryCount(flow.getRetryCount() + 1);
        flow.setErrorMessage(errorMessage);
        flow.setUpdatedAt(Instant.now());

        if (retryable) {
            int backoffSeconds = (int) Math.min(Math.pow(2, flow.getRetryCount()), 60);
            flow.setBackoffSeconds(backoffSeconds);
            flow.setNextRetryAt(Instant.now().plusSeconds(backoffSeconds));
            flow.setStatus(FlowStatus.WAITING_RETRY);
        } else {
            flow.setStatus(FlowStatus.FAILED);
        }
        flowRepository.save(flow);

        SagaStepLog stepLog = SagaStepLog.builder()
                .id(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .stepName(step.name())
                .status(retryable ? "RETRYING" : "FAILED")
                .attemptNumber(flow.getRetryCount())
                .requestPayload(requestPayload)
                .responsePayload("{\"error\": \"" + errorMessage.replace("\"", "'") + "\"}")
                .errorMessage(errorMessage)
                .startedAt(Instant.now())
                .completedAt(Instant.now())
                .build();
        stepLogRepository.save(stepLog);

        if (!retryable) {
            log.error("Non-retryable error on step {} for flow {}: {}", step, flow.getId(), errorMessage);
        } else {
            log.warn("Step {} failed for flow {} (attempt {}, routing to retry topic)",
                    step, flow.getId(), flow.getRetryCount());
        }
    }

    /**
     * Handles step result from the reply topic.
     * Atomically advances to the next step or marks complete.
     */
    @Transactional
    public void handleStepResult(StepResultMessage result) {
        DocumentFlow flow = flowRepository.findById(result.getFlowId())
                .orElseThrow(() -> new IllegalArgumentException("Flow not found: " + result.getFlowId()));

        if (result.isSuccess()) {
            flow.setRetryCount(0);
            flow.setBackoffSeconds(0);
            flow.setNextRetryAt(null);
            FlowStep nextStep = result.getStep().next();

            if (nextStep == null) {
                flow.setStatus(FlowStatus.COMPLETED);
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                log.info("Flow {} completed successfully", flow.getId());
            } else {
                flow.setCurrentStep(nextStep);
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                publishStepCommand(flow, nextStep);
            }
        } else {
            flow.setStatus(FlowStatus.FAILED);
            flow.setErrorMessage(result.getErrorMessage());
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
        }
    }

    /**
     * Called by DLT handler after all Kafka retries exhausted.
     */
    @Transactional
    public void markFlowDeadLettered(String flowId, FlowStep step, String errorMessage) {
        flowRepository.findById(flowId).ifPresent(flow -> {
            flow.setStatus(FlowStatus.FAILED);
            flow.setErrorMessage("[DLT] " + (errorMessage != null ? errorMessage : "exhausted retries"));
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);

            SagaStepLog stepLog = SagaStepLog.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .stepName(step != null ? step.name() : "UNKNOWN")
                    .status("DEAD_LETTERED")
                    .errorMessage("[DLT] " + errorMessage)
                    .startedAt(Instant.now())
                    .completedAt(Instant.now())
                    .build();
            stepLogRepository.save(stepLog);

            log.error("Flow {} dead-lettered at step {}", flowId, step);
        });
    }

    private void publishStepCommand(DocumentFlow flow, FlowStep step) {
        FlowCommandMessage command = FlowCommandMessage.builder()
                .eventId(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .step(step)
                .build();

        outboxService.saveEvent("DocumentFlow", flow.getId(),
                "STEP_COMMAND_" + step.name(), KafkaTopics.SAGA_STEPS, command);
    }

    private void publishStepResult(DocumentFlow flow, FlowStep step, boolean success,
                                   String errorMessage, boolean retryable) {
        StepResultMessage result = StepResultMessage.builder()
                .eventId(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .step(step)
                .success(success)
                .errorMessage(errorMessage)
                .retryable(retryable)
                .build();

        outboxService.saveEvent("DocumentFlow", flow.getId(),
                "STEP_RESULT_" + step.name(), KafkaTopics.SAGA_REPLIES, result);
    }

    private String serializeFlow(DocumentFlow flow) {
        try {
            return objectMapper.writeValueAsString(flow);
        } catch (Exception e) {
            return "{\"error\": \"serialization failed\"}";
        }
    }
}
