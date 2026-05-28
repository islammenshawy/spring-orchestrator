package com.orchestrator.starter.failover;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Blueprint for a Kafka listener container — stores everything needed
 * to recreate the container with different topic names on DC failover.
 */
@Data
@Builder
public class ContainerBlueprint {
    private final String id;
    private final String originalTopic;
    private final String groupId;
    private final Object messageListener;
    private final int concurrency;
    private final Map<String, Object> consumerProperties;
}
