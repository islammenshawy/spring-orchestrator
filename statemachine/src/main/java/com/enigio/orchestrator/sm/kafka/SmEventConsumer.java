package com.enigio.orchestrator.sm.kafka;

import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.common.domain.FlowStep;
import com.enigio.orchestrator.common.exception.RetryableException;
import com.enigio.orchestrator.common.idempotency.IdempotencyService;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.enigio.orchestrator.sm.actions.*;
import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import com.enigio.orchestrator.sm.machine.StateMachineService;
import com.enigio.orchestrator.sm.persistence.MongoStateMachinePersist;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * SM Kafka consumer — one step per Kafka message.
 * Uses the state machine for state validation and persistence,
 * but executes the action directly so exceptions propagate to Kafka retry.
 *
 * Same pattern as Saga and SI: retry-0 → retry-1 → retry-2 → DLT.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SmEventConsumer {

    private final DocumentFlowRepository flowRepository;
    private final MongoStateMachinePersist smPersist;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Actions (direct execution, not via SM engine)
    private final CreateDocumentAction createDocumentAction;
    private final UploadAttachmentAction uploadAttachmentAction;
    private final RequestSignatureAction requestSignatureAction;
    private final VerifySignatureAction verifySignatureAction;
    private final FinalizeDocumentAction finalizeDocumentAction;

    @KafkaListener(topics = KafkaTopics.SM_EVENTS, groupId = "statemachine-processor")
    public void onFlowEvent(String message,
                            @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(name = KafkaHeaders.OFFSET) long offset) {
        FlowCommandMessage command;
        try {
            command = objectMapper.readValue(message, FlowCommandMessage.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Check idempotency (optimization only — handlers have their own guards)
        if (idempotencyService.isProcessed(command.getEventId())) {
            log.info("[SM] Event {} already processed, skipping", command.getEventId());
            return;
        }

        DocumentFlow flow = flowRepository.findById(command.getFlowId()).orElse(null);
        if (flow == null || flow.getStatus() == FlowStatus.COMPLETED) return;

        FlowStep step = flow.getCurrentStep();
        log.info("[SM][topic={}][offset={}] Step {} for flow {}", topic, offset, step, flow.getId());

        flow.setStatus(FlowStatus.IN_PROGRESS);

        // Execute step — handler idempotency guards protect against duplicate API calls
        try {
            executeAction(step, flow);
        } catch (RetryableException e) {
            flow.setRetryCount(flow.getRetryCount() + 1);
            int backoff = (int) Math.min(Math.pow(2, flow.getRetryCount()), 60);
            flow.setBackoffSeconds(backoff);
            flow.setNextRetryAt(Instant.now().plusSeconds(backoff));
            flow.setStatus(FlowStatus.WAITING_RETRY);
            flow.setErrorMessage(e.getMessage());
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
            persistSmState(flow.getId(), mapStepToState(step));
            throw e; // Route to Kafka retry topic
        }

        // Success — persist SM state and advance
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setUpdatedAt(Instant.now());

        FlowStep nextStep = step.next();
        if (nextStep == null) {
            flow.setStatus(FlowStatus.COMPLETED);
            flowRepository.save(flow);
            persistSmState(flow.getId(), DocumentFlowStates.COMPLETED);
            log.info("[SM] Flow {} completed", flow.getId());
        } else {
            flow.setCurrentStep(nextStep);
            flowRepository.save(flow);
            persistSmState(flow.getId(), mapStepToState(nextStep));
            publishNextStep(flow);
        }

        // Mark as processed AFTER everything completes
        idempotencyService.tryMarkAsProcessed(command.getEventId());
    }

    @KafkaListener(topics = KafkaTopics.SM_EVENTS + "-dlt", groupId = "sm-dlt-handler")
    public void handleDlt(String message,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
        log.error("[SM][DLT][topic={}][offset={}] Dead letter", topic, offset);
        try {
            FlowCommandMessage command = objectMapper.readValue(message, FlowCommandMessage.class);
            flowRepository.findById(command.getFlowId()).ifPresent(flow -> {
                flow.setStatus(FlowStatus.FAILED);
                flow.setErrorMessage("[SM-DLT] Exhausted all retry attempts");
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                persistSmState(flow.getId(), DocumentFlowStates.FAILED);
            });
        } catch (Exception e) {
            log.error("[SM] DLT failed: {}", e.getMessage());
        }
    }

    private void executeAction(FlowStep step, DocumentFlow flow) {
        // Create a minimal state context for the action
        var context = new org.springframework.statemachine.support.DefaultStateContext<DocumentFlowStates, DocumentFlowEvents>(
                null, null, null, null, null, null, null, null, null, null, null);

        // We pass flow data via a simple wrapper since actions read from DB
        // Actions read flowId from extended state — we'll call them directly instead
        switch (step) {
            case CREATE_DOCUMENT -> executeDirectly(flow, createDocumentAction);
            case UPLOAD_ATTACHMENT -> executeDirectly(flow, uploadAttachmentAction);
            case REQUEST_SIGNATURE -> executeDirectly(flow, requestSignatureAction);
            case VERIFY_SIGNATURE -> executeDirectly(flow, verifySignatureAction);
            case FINALIZE_DOCUMENT -> executeDirectly(flow, finalizeDocumentAction);
        }
    }

    /**
     * Execute an action directly without the SM engine.
     * Actions read the flow from MongoDB by flowId, so we just need to ensure
     * the flowId is accessible. Since actions use flowRepository.findById(),
     * the flow is already persisted.
     */
    private void executeDirectly(DocumentFlow flow,
                                 org.springframework.statemachine.action.Action<DocumentFlowStates, DocumentFlowEvents> action) {
        // Actions expect flowId in extended state variables.
        // Create a minimal state context with the flowId.
        var variables = new java.util.HashMap<Object, Object>();
        variables.put("flowId", flow.getId());

        // Use a mock state context that provides the variables
        var extendedState = new org.springframework.statemachine.support.DefaultExtendedState(variables);
        var stateContext = new org.springframework.statemachine.support.DefaultStateContext<DocumentFlowStates, DocumentFlowEvents>(
                org.springframework.statemachine.StateContext.Stage.TRANSITION,
                null, null, extendedState, null, null, null, null, null, null, null);

        action.execute(stateContext);
    }

    private DocumentFlowStates mapStepToState(FlowStep step) {
        return switch (step) {
            case CREATE_DOCUMENT -> DocumentFlowStates.CREATING_DOCUMENT;
            case UPLOAD_ATTACHMENT -> DocumentFlowStates.UPLOADING_ATTACHMENT;
            case REQUEST_SIGNATURE -> DocumentFlowStates.REQUESTING_SIGNATURE;
            case VERIFY_SIGNATURE -> DocumentFlowStates.VERIFYING_SIGNATURE;
            case FINALIZE_DOCUMENT -> DocumentFlowStates.FINALIZING;
        };
    }

    private void persistSmState(String flowId, DocumentFlowStates state) {
        try {
            smPersist.write(new DefaultStateMachineContext<>(state, null, null, null), flowId);
        } catch (Exception e) {
            log.warn("[SM] Failed to persist SM state for {}: {}", flowId, e.getMessage());
        }
    }

    private void publishNextStep(DocumentFlow flow) {
        try {
            FlowCommandMessage cmd = FlowCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .correlationId(flow.getCorrelationId())
                    .step(flow.getCurrentStep())
                    .build();
            kafkaTemplate.send(KafkaTopics.SM_EVENTS, flow.getId(),
                    objectMapper.writeValueAsString(cmd)).get();
        } catch (Exception e) {
            log.error("[SM] Failed to publish next step for {}: {}", flow.getId(), e.getMessage());
        }
    }
}
