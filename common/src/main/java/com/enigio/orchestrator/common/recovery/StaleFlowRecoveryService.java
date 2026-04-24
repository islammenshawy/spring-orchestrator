package com.enigio.orchestrator.common.recovery;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Shared recovery service for all 3 patterns.
 *
 * Solves: container crashes between MongoDB save and Kafka publish.
 * When a step succeeds, the flow is saved to MongoDB with the next step set,
 * but the Kafka message to trigger that next step may never be sent (container died).
 *
 * This service periodically scans for flows that:
 * - Have status IN_PROGRESS (step succeeded, was about to publish next command)
 * - Haven't been updated for longer than stale-threshold-minutes
 *
 * For those flows, it re-publishes the current step command to Kafka.
 * Consumer-side idempotency prevents duplicate execution if the step already ran.
 *
 * Config:
 *   recovery.scan-interval-ms: 30000    (scan every 30s)
 *   recovery.stale-threshold-minutes: 5 (consider stale after 5 min)
 *   recovery.kafka-topic: enigio.saga.steps (which topic to publish to)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StaleFlowRecoveryService {

    private final DocumentFlowRepository flowRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${recovery.stale-threshold-minutes:5}")
    private int staleThresholdMinutes;

    @Value("${recovery.kafka-topic:}")
    private String recoveryTopic;

    @Scheduled(fixedDelayString = "${recovery.scan-interval-ms:30000}")
    public void recoverStaleFlows() {
        if (recoveryTopic == null || recoveryTopic.isBlank()) return;

        Instant threshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);

        // Find IN_PROGRESS flows that haven't been updated (stuck between DB save and Kafka publish)
        List<DocumentFlow> staleFlows = flowRepository
                .findByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, threshold);

        if (!staleFlows.isEmpty()) {
            log.info("[Recovery] Found {} stale IN_PROGRESS flows", staleFlows.size());
        }

        for (DocumentFlow flow : staleFlows) {
            try {
                log.info("[Recovery] Re-publishing step {} for flow {} (stale since {})",
                        flow.getCurrentStep(), flow.getId(), flow.getUpdatedAt());

                FlowCommandMessage command = FlowCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flow.getId())
                        .correlationId(flow.getCorrelationId())
                        .step(flow.getCurrentStep())
                        .build();

                kafkaTemplate.send(recoveryTopic, flow.getId(),
                        objectMapper.writeValueAsString(command)).get();

                // Touch updatedAt so we don't keep re-publishing
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
            } catch (Exception e) {
                log.error("[Recovery] Failed to recover flow {}: {}", flow.getId(), e.getMessage());
            }
        }
    }
}
