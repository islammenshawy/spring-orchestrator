package com.orchestrator.starter.kafka;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka consumer for step commands and replies.
 * Routes messages to the correct FlowOrchestrator by reading flowType from the message.
 *
 * Layer 1 idempotency: check BEFORE, mark AFTER.
 * Failed steps are NOT marked — retries work correctly via Kafka retry topics.
 */
@Slf4j
public class OrchestratorKafkaConsumer<F extends OrchestratorFlow> {

    private final FlowTypeRegistry registry;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;
    private final boolean replyMode;

    public OrchestratorKafkaConsumer(FlowTypeRegistry registry,
                                     IdempotencyService idempotencyService,
                                     ObjectMapper objectMapper,
                                     boolean replyMode) {
        this.registry = registry;
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
        this.replyMode = replyMode;
    }

    /**
     * Process a step command: execute the step.
     * Routes to the correct FlowOrchestrator by flowType in the message.
     */
    public void onStepCommand(String payload, String topic, long offset) {
        StepCommandMessage command;
        try {
            command = objectMapper.readValue(payload, StepCommandMessage.class);
        } catch (Exception e) {
            log.error("[topic={}][offset={}] Deserialization failed", topic, offset);
            throw new RuntimeException("Deserialization failed", e);
        }

        if (idempotencyService.isProcessed(command.getEventId())) {
            log.debug("[topic={}][offset={}] Event {} already processed, skipping",
                    topic, offset, command.getEventId());
            return;
        }

        // Route by flowType — backward compat: null flowType → single flow
        FlowTypeDescriptor descriptor = registry.resolve(command.getFlowType());
        FlowOrchestrator<?> orchestrator = descriptor.getOrchestrator();

        log.info("[topic={}][offset={}] Step {} for flow {} (type={})",
                topic, offset, command.getStepName(), command.getFlowId(),
                command.getFlowType() != null ? command.getFlowType() : "default");

        if (replyMode && descriptor.isReplyEnabled()) {
            orchestrator.executeStepOnly(command.getFlowId(), command.getStepName());
        } else {
            orchestrator.executeStep(command.getFlowId(), command.getStepName());
        }

        idempotencyService.tryProcess(command.getEventId());
    }

    /**
     * Process a step reply: advance the flow to the next step.
     */
    public void onStepReply(String payload, String topic, long offset) {
        StepReplyMessage reply;
        try {
            reply = objectMapper.readValue(payload, StepReplyMessage.class);
        } catch (Exception e) {
            log.error("[reply][topic={}][offset={}] Deserialization failed", topic, offset);
            throw new RuntimeException("Reply deserialization failed", e);
        }

        if (idempotencyService.isProcessed(reply.getEventId())) {
            return;
        }

        FlowTypeDescriptor descriptor = registry.resolve(reply.getFlowType());

        log.info("[reply][topic={}][offset={}] Step {} {} for flow {} (type={})",
                topic, offset, reply.getStepName(), reply.getStatus(), reply.getFlowId(),
                reply.getFlowType() != null ? reply.getFlowType() : "default");

        if ("COMPLETED".equals(reply.getStatus()) || "RECOVERED".equals(reply.getStatus())) {
            descriptor.getOrchestrator().advanceAfterReply(
                    reply.getFlowId(), reply.getStepName(), reply.getFlowSnapshot());
        }

        idempotencyService.tryProcess(reply.getEventId());
    }

    /**
     * Handle dead-lettered messages.
     */
    public void onDlt(String payload, String topic, long offset, String exceptionMessage) {
        log.error("[DLT][topic={}][offset={}] Dead letter: {}", topic, offset, exceptionMessage);
        try {
            StepCommandMessage command = objectMapper.readValue(payload, StepCommandMessage.class);
            FlowTypeDescriptor descriptor = registry.resolve(command.getFlowType());
            String reason = exceptionMessage != null ? exceptionMessage : "unknown";
            descriptor.getOrchestrator().markDeadLettered(
                    command.getFlowId(), command.getStepName(), reason);
            idempotencyService.tryProcess(command.getEventId());
        } catch (Exception e) {
            log.error("DLT processing failed: {}", e.getMessage());
        }
    }

    public void onDlt(String payload, String topic, long offset) {
        onDlt(payload, topic, offset, null);
    }
}
