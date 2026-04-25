package com.orchestrator.starter.kafka;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

/**
 * Generic Kafka consumer for orchestrated flows.
 * Handles: deserialization, Layer 1 idempotency, step execution, Layer 1 post-mark.
 *
 * Not a @Component — instantiated by auto-configuration with the right
 * topic name and flow type. Users don't touch this class.
 */
@Slf4j
@RequiredArgsConstructor
public class OrchestratorKafkaConsumer<F extends OrchestratorFlow> {

    private final FlowOrchestrator<F> orchestrator;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    /**
     * Process a step command message.
     * Called by the @KafkaListener configured in auto-config.
     */
    public void onStepCommand(String payload, String topic, long offset) {
        StepCommandMessage command;
        try {
            command = objectMapper.readValue(payload, StepCommandMessage.class);
        } catch (Exception e) {
            log.error("[topic={}][offset={}] Deserialization failed", topic, offset);
            throw new RuntimeException("Deserialization failed", e);
        }

        // Layer 1: fast-path skip if already fully processed
        if (idempotencyService.isProcessed(command.getEventId())) {
            log.debug("Event {} already processed, skipping", command.getEventId());
            return;
        }

        log.info("[topic={}][offset={}] Step {} for flow {}",
                topic, offset, command.getStepName(), command.getFlowId());

        // Execute — passes step name from message (supports parallel steps)
        orchestrator.executeStep(command.getFlowId(), command.getStepName());

        // Layer 1: mark processed AFTER successful completion
        idempotencyService.markProcessed(command.getEventId());
    }

    /**
     * Handle dead-lettered messages after all retries exhausted.
     */
    public void onDlt(String payload, String topic, long offset) {
        log.error("[DLT][topic={}][offset={}] Dead letter received", topic, offset);
        try {
            StepCommandMessage command = objectMapper.readValue(payload, StepCommandMessage.class);
            orchestrator.markDeadLettered(command.getFlowId());
            idempotencyService.markProcessed(command.getEventId());
        } catch (Exception e) {
            log.error("DLT processing failed: {}", e.getMessage());
        }
    }
}
