package com.orchestrator.starter.recovery;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.kafka.StepCommandMessage;
import tools.jackson.databind.ObjectMapper;
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
 * - Caps recovery at maxRecoveryAttempts to prevent infinite recovery loops
 */
@Slf4j
public class StaleFlowRecoveryService {

    private final FlowTypeRegistry registry;
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int staleThresholdMinutes;
    private final int maxRecoveryAttempts;
    private final com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository;
    private final OrchestratorMetrics metrics;

    public StaleFlowRecoveryService(FlowTypeRegistry registry, KafkaTemplate kafkaTemplate,
                                     ObjectMapper objectMapper, int staleThresholdMinutes,
                                     com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository) {
        this(registry, kafkaTemplate, objectMapper, staleThresholdMinutes, 10, outboxRepository, null);
    }

    public StaleFlowRecoveryService(FlowTypeRegistry registry, KafkaTemplate kafkaTemplate,
                                     ObjectMapper objectMapper, int staleThresholdMinutes,
                                     int maxRecoveryAttempts,
                                     com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository,
                                     OrchestratorMetrics metrics) {
        this.registry = registry;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.staleThresholdMinutes = staleThresholdMinutes;
        this.maxRecoveryAttempts = maxRecoveryAttempts;
        this.outboxRepository = outboxRepository;
        this.metrics = metrics != null ? metrics : OrchestratorMetrics.noop();
    }

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

            recoverFlowType(descriptor.getFlowType(), flowRepository, commandTopic,
                    threshold, descriptor.getStepRegistry());
        }
    }

    @SuppressWarnings("unchecked")
    private void recoverFlowType(String flowType,
                                  OrchestratorFlowRepository<OrchestratorFlow> flowRepository,
                                  String commandTopic, Instant threshold,
                                  StepRegistry<?> stepRegistry) {
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
            // Recovery loop detection: cap at maxRecoveryAttempts
            if (flow.getRecoveryCount() >= maxRecoveryAttempts) {
                log.error("[Recovery] Flow {} exceeded max recovery attempts ({}) — marking FAILED",
                        flow.getId(), maxRecoveryAttempts);
                flow.setStatus(FlowStatus.FAILED);
                flow.setErrorMessage("Exceeded max recovery attempts (" + maxRecoveryAttempts + ")");
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                continue;
            }

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
                flow.setRecoveryCount(flow.getRecoveryCount() + 1);
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                metrics.recoveryRecovered(flowType);
                log.info("[Recovery] Re-published step {} for flow {} (type: {}, attempt: {})",
                        flow.getCurrentStep(), flow.getId(), flowType, flow.getRecoveryCount());
            } catch (Exception e) {
                log.error("[Recovery] Failed to recover flow {} (type: {}): {}",
                        flow.getId(), flowType, e.getMessage());
            }
        }

        // Expire WAITING_RETRY flows that exceeded step-level expiresAfter
        expireWaitingFlows(flowType, flowRepository, stepRegistry);
    }

    private void expireWaitingFlows(String flowType,
                                     OrchestratorFlowRepository<OrchestratorFlow> flowRepository,
                                     StepRegistry<?> stepRegistry) {
        List<OrchestratorFlow> waitingFlows = flowRepository
                .findByStatus(FlowStatus.WAITING_RETRY);

        for (OrchestratorFlow flow : waitingFlows) {
            Instant waitingSince = flow.getWaitingSince();
            if (waitingSince == null) continue;

            String stepName = flow.getCurrentStep();
            StepHandler<?> handler = stepRegistry != null ? stepRegistry.getHandler(stepName) : null;
            if (handler == null) continue;

            java.time.Duration expiresAfter = handler.getExpiresAfter();
            if (expiresAfter == null) continue;

            if (waitingSince.plus(expiresAfter).isBefore(Instant.now())) {
                long waitedHours = java.time.Duration.between(waitingSince, Instant.now()).toHours();
                flow.setStatus(FlowStatus.FAILED);
                flow.setErrorMessage("Step " + stepName + " expired after " + waitedHours +
                        "h (limit: " + expiresAfter.toHours() + "h)");
                flow.setUpdatedAt(Instant.now());
                flowRepository.save(flow);
                metrics.flowFailed(flowType);
                log.info("[Recovery] Expired flow {} at step {} (waited {}h, limit {}h)",
                        flow.getId(), stepName, waitedHours, expiresAfter.toHours());
            }
        }
    }
}
