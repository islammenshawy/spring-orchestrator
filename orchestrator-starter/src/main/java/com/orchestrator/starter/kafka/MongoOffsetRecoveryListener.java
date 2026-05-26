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

        // Always check MongoDB for every partition — MongoDB is the cross-DC source of truth.
        // Kafka's __consumer_offsets may be stale (from a previous deployment on this cluster)
        // or missing (first time on this cluster). MongoDB offsets are replicated cross-DC.
        Collection<TopicPartition> stillNeedsRecovery = new java.util.ArrayList<>();

        for (TopicPartition partition : partitions) {
            // Check across ALL clusters — on failover, the latest offset may be from another cluster
            MongoOffsetStore.StoredOffset stored = offsetStore.getLatestOffsetAcrossClusters(
                    consumerGroup, partition.topic(), partition.partition());

            if (stored != null) {
                var committed = consumer.committed(java.util.Set.of(partition));
                var kafkaOffset = committed.get(partition);

                if (kafkaOffset != null) {
                    // Both Kafka and MongoDB have offsets — compare to detect stale Kafka offset.
                    // If MongoDB's stored offset is ahead (newer timestamp), use MongoDB.
                    // This handles DC failover where Cluster B has old stale Kafka offsets.
                    long kafkaPosition = kafkaOffset.offset();
                    long mongoPosition = stored.getOffset();

                    if (mongoPosition > kafkaPosition) {
                        log.warn("[OffsetRecovery] {} — MongoDB offset ({}) ahead of Kafka offset ({}). " +
                                        "Kafka offset is stale — recovering from MongoDB (timestamp={})",
                                partition, mongoPosition, kafkaPosition,
                                java.time.Instant.ofEpochMilli(stored.getMessageTimestamp()));
                        seekByStoredTimestamp(consumer, partition, stored);
                    } else {
                        // Kafka offset is at or ahead of MongoDB — Kafka is authoritative
                        log.debug("[OffsetRecovery] {} — Kafka offset ({}) is current, no recovery needed",
                                partition, kafkaPosition);
                    }
                } else {
                    // No Kafka offset — use MongoDB (classic failover case)
                    log.info("[OffsetRecovery] {} — no Kafka offset, recovering from MongoDB (timestamp={})",
                            partition, java.time.Instant.ofEpochMilli(stored.getMessageTimestamp()));
                    seekByStoredTimestamp(consumer, partition, stored);
                }
            } else {
                // Not in MongoDB — check if Kafka has it
                var committed = consumer.committed(java.util.Set.of(partition));
                if (committed.get(partition) == null) {
                    stillNeedsRecovery.add(partition);
                }
                // If Kafka has an offset but MongoDB doesn't, trust Kafka (normal single-cluster operation)
            }
        }

        // Delegate remaining partitions to timestamp/earliest/latest fallback
        if (!stillNeedsRecovery.isEmpty()) {
            log.info("[OffsetRecovery] {} partition(s) not in Kafka or MongoDB — using fallback strategy",
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
