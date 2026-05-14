package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.outbox.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Reports orchestrator-specific health for Kubernetes probes.
 * DOWN if outbox is backing up or dead-lettered events exist.
 */
@RequiredArgsConstructor
public class OrchestratorHealthIndicator implements HealthIndicator {

    private final OutboxEventRepository outboxRepository;
    private final int outboxThreshold;

    @Override
    public Health health() {
        long pending = outboxRepository.countByPublishedFalseAndDeadLetteredFalse();
        long deadLettered = outboxRepository.countByDeadLetteredTrue();

        Health.Builder builder = (pending > outboxThreshold || deadLettered > 0)
                ? Health.down() : Health.up();

        return builder
                .withDetail("outboxPending", pending)
                .withDetail("outboxDeadLettered", deadLettered)
                .withDetail("outboxThreshold", outboxThreshold)
                .build();
    }
}
