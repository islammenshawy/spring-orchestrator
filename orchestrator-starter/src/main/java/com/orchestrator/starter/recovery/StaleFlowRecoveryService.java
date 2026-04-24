package com.orchestrator.starter.recovery;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.kafka.StepCommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Consumer-side idempotency + handler idempotency prevent duplicate execution.
 */
@Slf4j
@RequiredArgsConstructor
public class StaleFlowRecoveryService<F extends OrchestratorFlow> {

    private final OrchestratorFlowRepository<F> flowRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String commandTopic;
    private final int staleThresholdMinutes;

    @Scheduled(fixedDelayString = "${orchestrator.recovery.scan-interval-ms:30000}")
    public void recoverStaleFlows() {
        if (commandTopic == null || commandTopic.isBlank()) return;

        Instant threshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);
        List<F> staleFlows = flowRepository
                .findByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, threshold);

        if (!staleFlows.isEmpty()) {
            log.info("[Recovery] Found {} stale flows", staleFlows.size());
        }

        for (F flow : staleFlows) {
            try {
                StepCommandMessage cmd = StepCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flow.getId())
                        .correlationId(flow.getCorrelationId())
                        .stepName(flow.getCurrentStep())
                        .build();
                kafkaTemplate.send(commandTopic, flow.getId(),
                        objectMapper.writeValueAsString(cmd)).get();
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                log.info("[Recovery] Re-published step {} for flow {}", flow.getCurrentStep(), flow.getId());
            } catch (Exception e) {
                log.error("[Recovery] Failed to recover flow {}: {}", flow.getId(), e.getMessage());
            }
        }
    }
}
