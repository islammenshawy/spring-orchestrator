package com.enigio.orchestrator.saga.kafka;

import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.idempotency.IdempotencyService;
import com.enigio.orchestrator.common.kafka.StepResultMessage;
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
public class SagaReplyConsumer {

    private final SagaOrchestrator orchestrator;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.SAGA_REPLIES, groupId = "saga-orchestrator")
    public void onStepResult(String message,
                             @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(name = KafkaHeaders.OFFSET) long offset) {
        try {
            StepResultMessage result = objectMapper.readValue(message, StepResultMessage.class);

            if (idempotencyService.isProcessed(result.getEventId())) {
                log.info("Reply {} already processed, skipping", result.getEventId());
                return;
            }

            log.info("[topic={}][offset={}] Step result for flow {}, step {}, success={}",
                    topic, offset, result.getFlowId(), result.getStep(), result.isSuccess());
            orchestrator.handleStepResult(result);

            // Mark processed AFTER handling completes
            idempotencyService.tryMarkAsProcessed(result.getEventId());
        } catch (Exception e) {
            log.error("[topic={}][offset={}] Error: {}", topic, offset, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = KafkaTopics.SAGA_REPLIES + "-dlt", groupId = "saga-replies-dlt-handler")
    public void handleDlt(String message,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
        log.error("[DLT][topic={}][offset={}] Dead letter reply", topic, offset);
    }
}
