package com.orchestrator.starter.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;

/**
 * Rebalance and partition metrics via Micrometer.
 * Only instantiated if MeterRegistry is available (actuator present).
 */
public class KafkaMetricsService {

    @Getter private final Counter revokedCounter;
    @Getter private final Counter assignedCounter;
    @Getter private final Counter lostCounter;

    public KafkaMetricsService(MeterRegistry registry, PartitionAssignmentTracker tracker) {
        this.revokedCounter = Counter.builder("orchestrator.kafka.rebalance.total")
                .tag("event", "revoked")
                .description("Number of partition revoke events")
                .register(registry);

        this.assignedCounter = Counter.builder("orchestrator.kafka.rebalance.total")
                .tag("event", "assigned")
                .description("Number of partition assign events")
                .register(registry);

        this.lostCounter = Counter.builder("orchestrator.kafka.rebalance.total")
                .tag("event", "lost")
                .description("Number of partition lost events")
                .register(registry);

        Gauge.builder("orchestrator.kafka.partitions.assigned", tracker, PartitionAssignmentTracker::count)
                .description("Current number of partitions assigned to this instance")
                .register(registry);
    }
}
