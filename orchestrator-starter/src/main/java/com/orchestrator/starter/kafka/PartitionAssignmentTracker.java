package com.orchestrator.starter.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe tracker of current Kafka partition assignments.
 * Updated by the rebalance listener, queryable by health checks and metrics.
 */
public class PartitionAssignmentTracker {

    private final Set<TopicPartition> assignments = ConcurrentHashMap.newKeySet();

    public void assigned(Collection<TopicPartition> partitions) {
        assignments.addAll(partitions);
    }

    public void revoked(Collection<TopicPartition> partitions) {
        assignments.removeAll(partitions);
    }

    public void lost(Collection<TopicPartition> partitions) {
        assignments.removeAll(partitions);
    }

    public Set<TopicPartition> current() {
        return Collections.unmodifiableSet(assignments);
    }

    public int count() {
        return assignments.size();
    }
}
