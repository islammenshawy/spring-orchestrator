package com.orchestrator.starter.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class OrchestratorRebalanceListener implements ConsumerAwareRebalanceListener {

    private final PartitionAssignmentTracker tracker;

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        log.info("[Rebalance] REVOKING partitions (before commit): {}", format(partitions));
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        log.info("[Rebalance] REVOKED partitions (after commit): {}", format(partitions));
        tracker.revoked(partitions);
        log.info("[Rebalance] Active partitions: {}", tracker.count());
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        log.info("[Rebalance] ASSIGNED partitions: {}", format(partitions));
        tracker.assigned(partitions);
        log.info("[Rebalance] Active partitions: {}", tracker.count());
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        log.warn("[Rebalance] LOST partitions (broker-initiated): {}", format(partitions));
        tracker.lost(partitions);
        log.info("[Rebalance] Active partitions: {}", tracker.count());
    }

    private String format(Collection<TopicPartition> partitions) {
        return partitions.stream()
                .map(tp -> tp.topic() + "-" + tp.partition())
                .collect(Collectors.joining(", "));
    }
}
