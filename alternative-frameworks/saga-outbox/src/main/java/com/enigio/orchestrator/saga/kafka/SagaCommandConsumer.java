package com.enigio.orchestrator.saga.kafka;

import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.idempotency.IdempotencyService;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.enigio.orchestrator.saga.saga.SagaOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaCommandConsumer {

    private final SagaOrchestrator orchestrator;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    /**
     * Idempotency check happens AFTER step execution, not before.
     *
     * Why: if we mark as processed before executing, and the container crashes
     * mid-step, the message is redelivered but skipped — step never completes.
     *
     * Safe because: each step handler has its own idempotency guard
     * (checks if enigioDocumentId/attachmentId/etc already exists before calling vendor).
     * So redelivered messages re-enter the step, the handler sees the result
     * is already set, skips the API call, and completes normally.
     *
     * Flow on rebalance/crash:
     *   1. Message delivered → step executes → API called → result saved to MongoDB
     *   2. Container crashes before processed_events write
     *   3. New instance gets partition → message redelivered
     *   4. Step handler: "enigioDocumentId already set, skipping API call"
     *   5. Step completes → processed_events written → done
     */
    @KafkaListener(topics = KafkaTopics.SAGA_STEPS, groupId = "saga-step-executor")
    public void onStepCommand(String message,
                              @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(name = KafkaHeaders.OFFSET) long offset) {
        FlowCommandMessage command;
        try {
            command = objectMapper.readValue(message, FlowCommandMessage.class);
        } catch (Exception e) {
            log.error("[topic={}][offset={}] Deserialization failed", topic, offset);
            throw new RuntimeException("Deserialization failed", e);
        }

        // Check if already fully processed (optimization — skip if we know it's done)
        if (idempotencyService.isProcessed(command.getEventId())) {
            log.info("Event {} already processed, skipping", command.getEventId());
            return;
        }

        log.info("[topic={}][offset={}] Executing step {} for flow {}",
                topic, offset, command.getStep(), command.getFlowId());

        // Execute step — handler idempotency guards protect against duplicate API calls
        orchestrator.executeStep(command.getFlowId(), command.getStep());

        // Mark as processed AFTER successful completion
        idempotencyService.tryMarkAsProcessed(command.getEventId());
    }

    @KafkaListener(topics = KafkaTopics.SAGA_STEPS + "-dlt", groupId = "saga-dlt-handler")
    public void handleDlt(String message,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
        log.error("[DLT][topic={}][offset={}] Dead letter received", topic, offset);
        try {
            FlowCommandMessage command = objectMapper.readValue(message, FlowCommandMessage.class);
            orchestrator.markFlowDeadLettered(command.getFlowId(), command.getStep(),
                    "Exhausted all retry attempts");
            idempotencyService.tryMarkAsProcessed(command.getEventId());
        } catch (Exception e) {
            log.error("Failed to process DLT message: {}", e.getMessage());
        }
    }
}
