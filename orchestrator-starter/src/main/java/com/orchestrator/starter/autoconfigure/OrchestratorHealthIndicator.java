package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Reports orchestrator-specific health for Kubernetes probes.
 * DOWN if outbox is backing up, dead-lettered events exist,
 * flows are stale, or compensation has failed.
 */
public class OrchestratorHealthIndicator implements HealthIndicator {

    private final OutboxEventRepository outboxRepository;
    private final FlowTypeRegistry registry;
    private final int outboxThreshold;
    private final int staleThresholdMinutes;

    public OrchestratorHealthIndicator(OutboxEventRepository outboxRepository,
                                        FlowTypeRegistry registry,
                                        int outboxThreshold,
                                        int staleThresholdMinutes) {
        this.outboxRepository = outboxRepository;
        this.registry = registry;
        this.outboxThreshold = outboxThreshold;
        this.staleThresholdMinutes = staleThresholdMinutes;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Health health() {
        long pending = outboxRepository.countByPublishedFalseAndDeadLetteredFalse();
        long deadLettered = outboxRepository.countByDeadLetteredTrue();

        // Count stale and compensation-failed flows across all flow types (uses count queries, not find)
        long staleFlows = 0;
        Instant staleThreshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);
        for (FlowTypeDescriptor desc : registry.getAll()) {
            OrchestratorFlowRepository<?> repo = desc.getRepository();
            if (repo == null) continue;
            staleFlows += repo.countByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, staleThreshold);
            try {
                staleFlows += repo.countByStatusAndUpdatedAtBefore(FlowStatus.COMPENSATION_FAILED, staleThreshold);
            } catch (Exception ignored) {}
        }

        // Only outbox issues cause DOWN — stale flows are an operational signal, not a pod health issue.
        // Stale flows are reported as detail for dashboards/alerts but don't affect readiness.
        boolean down = pending > outboxThreshold || deadLettered > 0;

        Health.Builder builder = down ? Health.down() : Health.up();

        return builder
                .withDetail("outboxPending", pending)
                .withDetail("outboxDeadLettered", deadLettered)
                .withDetail("outboxThreshold", outboxThreshold)
                .withDetail("staleFlows", staleFlows)
                .build();
    }
}
