package com.orchestrator.starter.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Recovers consumer offsets from MongoDB on partition assignment.
 *
 * Designed for multi-cluster DC failover where:
 *   - Messages are replicated via MirrorMaker/Cluster Linking
 *   - __consumer_offsets are NOT replicated (per-cluster)
 *   - Offset numbers may differ between clusters
 *
 * Recovery strategy:
 *   1. On partition assignment, check __consumer_offsets (Kafka-native)
 *   2. If no committed offset → check MongoDB offset store
 *   3. If MongoDB has a stored offset:
 *      a. Use stored messageTimestamp to find position via offsetsForTimes()
 *         (timestamp is cluster-independent, unlike offset numbers)
 *      b. Seek to that position + 1 (the message after the last processed)
 *   4. If MongoDB also has nothing → fall back to configured strategy
 *      (TIMESTAMP/EARLIEST/LATEST via TimestampOffsetRecoveryListener)
 *
 * Why timestamp instead of offset number:
 *   Offset 1164 on Cluster A might be offset 1170 on Cluster B.
 *   But timestamp 1714500000000 maps to the same logical message on both.
 *   offsetsForTimes() uses the broker's timestamp index for O(log n) lookup.
 */
@Slf4j
public class MongoOffsetRecoveryListener implements ConsumerAwareRebalanceListener {

    private final MongoOffsetStore offsetStore;
    private final String consumerGroup;
    private final TimestampOffsetRecoveryListener fallbackListener;

    public MongoOffsetRecoveryListener(MongoOffsetStore offsetStore,
                                       String consumerGroup,
                                       TimestampOffsetRecoveryListener fallbackListener) {
        this.offsetStore = offsetStore;
        this.consumerGroup = consumerGroup;
        this.fallbackListener = fallbackListener;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        // Split partitions into those with Kafka offsets and those without
        Collection<TopicPartition> needsRecovery = new java.util.ArrayList<>();

        for (TopicPartition partition : partitions) {
            var committed = consumer.committed(java.util.Set.of(partition));
            if (committed.get(partition) != null) {
                // Kafka has the offset — normal operation, no recovery needed
                continue;
            }
            needsRecovery.add(partition);
        }

        if (needsRecovery.isEmpty()) return;

        log.info("[OffsetRecovery] {} partition(s) with no Kafka offset — checking MongoDB",
                needsRecovery.size());

        // For partitions without Kafka offsets, try MongoDB
        Collection<TopicPartition> stillNeedsRecovery = new java.util.ArrayList<>();

        for (TopicPartition partition : needsRecovery) {
            MongoOffsetStore.StoredOffset stored = offsetStore.getLastOffset(
                    consumerGroup, partition.topic(), partition.partition());

            if (stored != null) {
                // Found in MongoDB — seek by timestamp (cluster-independent)
                seekByStoredTimestamp(consumer, partition, stored);
            } else {
                // Not in MongoDB either — delegate to fallback
                stillNeedsRecovery.add(partition);
            }
        }

        // Delegate remaining partitions to timestamp/earliest/latest fallback
        if (!stillNeedsRecovery.isEmpty()) {
            log.info("[OffsetRecovery] {} partition(s) not in MongoDB — using fallback strategy",
                    stillNeedsRecovery.size());
            fallbackListener.onPartitionsAssigned(consumer, stillNeedsRecovery);
        }
    }

    private void seekByStoredTimestamp(Consumer<?, ?> consumer, TopicPartition partition,
                                       MongoOffsetStore.StoredOffset stored) {
        try {
            // Use the stored message timestamp to find position on this cluster
            Map<TopicPartition, Long> timestamps = new HashMap<>();
            timestamps.put(partition, stored.getMessageTimestamp());

            Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestamps);
            OffsetAndTimestamp found = result.get(partition);

            if (found != null) {
                // Seek to the found offset (the message at or after the stored timestamp)
                // The message at this timestamp may be the last processed one or the next one
                // Idempotency guard handles if we re-process the last one
                consumer.seek(partition, found.offset());
                log.info("[OffsetRecovery] {} → offset {} via MongoDB (stored eventId={}, " +
                                "storedOffset={} on source cluster, timestamp={})",
                        partition, found.offset(), stored.getEventId(),
                        stored.getOffset(), java.time.Instant.ofEpochMilli(stored.getMessageTimestamp()));
            } else {
                // Timestamp too old — all messages in retention window are newer
                // This means the last processed message has been deleted by retention
                // Seek to beginning of what's available
                consumer.seekToBeginning(List.of(partition));
                log.warn("[OffsetRecovery] {} — stored timestamp too old (message expired), seeked to BEGINNING",
                        partition);
            }
        } catch (Exception e) {
            log.error("[OffsetRecovery] Failed to recover {} from MongoDB — using fallback: {}",
                    partition, e.getMessage());
            fallbackListener.onPartitionsAssigned(consumer, List.of(partition));
        }
    }
}
