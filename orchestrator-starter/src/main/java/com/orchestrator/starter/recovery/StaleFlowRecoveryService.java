package com.orchestrator.starter.recovery;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.kafka.StepCommandMessage;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Recovers flows stuck in IN_PROGRESS after a container crash.
 * Re-publishes the current step command to Kafka.
 *
 * Iterates ALL registered flow types — each flow type has its own
 * repository and command topic.
 *
 * Guards against false positives:
 * - Skips flows with pending outbox events (pipeline is just busy)
 * - Uses configurable stale threshold (default 15 min, must exceed retry budget)
 */
@Slf4j
@RequiredArgsConstructor
public class StaleFlowRecoveryService {

    private final FlowTypeRegistry registry;
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int staleThresholdMinutes;
    private final com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository;

    @Scheduled(fixedDelayString = "${orchestrator.recovery.scan-interval-ms:30000}")
    @SuppressWarnings("unchecked")
    public void recoverStaleFlows() {
        Instant threshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);

        for (FlowTypeDescriptor descriptor : registry.getAll()) {
            String commandTopic = descriptor.getCommandTopic();
            if (commandTopic == null || commandTopic.isBlank()) continue;

            OrchestratorFlowRepository<OrchestratorFlow> flowRepository =
                    (OrchestratorFlowRepository<OrchestratorFlow>) descriptor.getRepository();
            if (flowRepository == null) continue;

            recoverFlowType(descriptor.getFlowType(), flowRepository, commandTopic, threshold);
        }
    }

    private void recoverFlowType(String flowType,
                                  OrchestratorFlowRepository<OrchestratorFlow> flowRepository,
                                  String commandTopic, Instant threshold) {
        List<OrchestratorFlow> staleFlows = flowRepository
                .findByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, threshold);

        // Filter out flows with pending outbox events — pipeline is just busy, not stuck
        if (outboxRepository != null) {
            staleFlows = staleFlows.stream()
                    .filter(f -> outboxRepository.countByFlowIdAndPublishedFalse(f.getId()) == 0)
                    .toList();
        }

        if (!staleFlows.isEmpty()) {
            log.info("[Recovery] Found {} truly stale flows for type '{}' (no pending outbox)",
                    staleFlows.size(), flowType);
        }

        for (OrchestratorFlow flow : staleFlows) {
            try {
                StepCommandMessage cmd = StepCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flow.getId())
                        .correlationId(flow.getCorrelationId())
                        .stepName(flow.getCurrentStep())
                        .flowType(flowType)
                        .build();
                String partitionKey = flow.getCorrelationId() != null
                        ? flow.getCorrelationId() : flow.getId();
                kafkaTemplate.send(commandTopic, partitionKey,
                        objectMapper.writeValueAsString(cmd)).get();
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                log.info("[Recovery] Re-published step {} for flow {} (type: {})",
                        flow.getCurrentStep(), flow.getId(), flowType);
            } catch (Exception e) {
                log.error("[Recovery] Failed to recover flow {} (type: {}): {}",
                        flow.getId(), flowType, e.getMessage());
            }
        }
    }
}
