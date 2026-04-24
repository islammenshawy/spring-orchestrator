package com.enigio.orchestrator.si.flow;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.common.exception.RetryableException;
import com.enigio.orchestrator.common.idempotency.IdempotencyService;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka consumer for Spring Integration pattern.
 * Retry/DLT managed by RetryTopicConfiguration bean (SiKafkaRetryConfig)
 * — same Kafka retry topic pattern as Saga/SM: retry-0 → retry-1 → retry-2 → DLT.
 *
 * On each Kafka message:
 * 1. Load flow from MongoDB
 * 2. Execute current step via IntegrationFlow
 * 3. On success → publish next step command to Kafka
 * 4. On RetryableException → thrown to Spring Kafka → routed to retry topic
 * 5. After all retries exhausted → DLT handler marks FAILED
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaInboundConfig {

    private final DocumentFlowRepository flowRepository;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;
    private final MessageChannel enigioInputChannel;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "enigio.si.commands", groupId = "si-processor")
    public void onStepCommand(String payload,
                              @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(name = KafkaHeaders.OFFSET) long offset) {
        FlowCommandMessage command;
        try {
            command = objectMapper.readValue(payload, FlowCommandMessage.class);
        } catch (Exception e) {
            log.error("[SI][topic={}][offset={}] Deserialization failed", topic, offset);
            throw new RuntimeException(e);
        }

        if (idempotencyService.isProcessed(command.getEventId())) {
            log.info("[SI] Event {} already processed, skipping", command.getEventId());
            return;
        }

        DocumentFlow flow = flowRepository.findById(command.getFlowId()).orElse(null);
        if (flow == null) {
            log.warn("[SI] Flow not found: {}", command.getFlowId());
            return;
        }
        if (flow.getStatus() == FlowStatus.COMPLETED) {
            log.info("[SI] Flow {} already completed, skipping", flow.getId());
            return;
        }

        log.info("[SI][topic={}][offset={}] Step {} for flow {}",
                topic, offset, flow.getCurrentStep(), flow.getId());

        // Execute step via IntegrationFlow — may throw RetryableException
        try {
            enigioInputChannel.send(new GenericMessage<>(flow));
        } catch (Exception e) {
            Throwable cause = unwrap(e);
            if (cause instanceof RetryableException re) {
                // Track retry state in MongoDB for UI visibility
                flow.setRetryCount(flow.getRetryCount() + 1);
                int backoff = (int) Math.min(Math.pow(2, flow.getRetryCount()), 60);
                flow.setBackoffSeconds(backoff);
                flow.setNextRetryAt(Instant.now().plusSeconds(backoff));
                flow.setStatus(FlowStatus.WAITING_RETRY);
                flow.setErrorMessage(cause.getMessage());
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                // Re-throw so Spring Kafka routes to retry topic
                throw re;
            }
            throw new RuntimeException(cause);
        }

        // Reload flow after IntegrationFlow updated it
        flow = flowRepository.findById(command.getFlowId()).orElse(flow);

        // If not completed, publish next step command
        if (flow.getStatus() != FlowStatus.COMPLETED) {
            publishNextStep(flow);
        }

        // Mark as processed AFTER everything completes
        idempotencyService.tryMarkAsProcessed(command.getEventId());
    }

    @KafkaListener(topics = "enigio.si.commands-dlt", groupId = "si-dlt-handler")
    public void handleDlt(String payload,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(name = KafkaHeaders.OFFSET) long offset) {
        log.error("[SI][DLT][topic={}][offset={}] Dead letter", topic, offset);
        try {
            FlowCommandMessage command = objectMapper.readValue(payload, FlowCommandMessage.class);
            flowRepository.findById(command.getFlowId()).ifPresent(flow -> {
                flow.setStatus(FlowStatus.FAILED);
                flow.setErrorMessage("[SI-DLT] Exhausted all retry attempts");
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
            });
        } catch (Exception e) {
            log.error("[SI] DLT processing failed: {}", e.getMessage());
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
            kafkaTemplate.send("enigio.si.commands", flow.getId(),
                    objectMapper.writeValueAsString(cmd)).get();
        } catch (Exception e) {
            log.error("[SI] Failed to publish next step for flow {}: {}", flow.getId(), e.getMessage());
        }
    }

    private Throwable unwrap(Throwable t) {
        while (t.getCause() != null && !(t instanceof RetryableException)) {
            t = t.getCause();
        }
        return t;
    }
}
