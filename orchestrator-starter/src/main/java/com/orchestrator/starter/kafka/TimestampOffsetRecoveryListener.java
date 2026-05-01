package com.orchestrator.starter.kafka;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles offset recovery when committed offsets are lost (consumer group expired,
 * broker corruption, cluster migration).
 *
 * Instead of replaying from the beginning (earliest) which floods vendors with
 * stale requests, this listener seeks to a configurable time window using
 * Kafka's timestamp index via offsetsForTimes().
 *
 * Strategy:
 *   1. On partition assignment, check if committed offset exists
 *   2. If no committed offset found (lost), apply fallback:
 *      - TIMESTAMP: seek to (now - offsetFallbackHours) — only replay recent messages
 *      - EARLIEST: seek to beginning (safe but noisy)
 *      - LATEST: seek to end (fast but skips messages)
 *   3. Idempotency guards (Layer 1 + Layer 2) handle any duplicates from replay
 *
 * Config:
 *   orchestrator.recovery.offset-fallback: TIMESTAMP (default)
 *   orchestrator.recovery.offset-fallback-hours: 24 (default)
 */
@Slf4j
public class TimestampOffsetRecoveryListener implements ConsumerAwareRebalanceListener {

    private final OrchestratorProperties.OffsetFallback fallback;
    private final int fallbackHours;

    public TimestampOffsetRecoveryListener(OrchestratorProperties.RecoveryConfig config) {
        this.fallback = config.getOffsetFallback();
        this.fallbackHours = config.getOffsetFallbackHours();
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        for (TopicPartition partition : partitions) {
            var committed = consumer.committed(java.util.Set.of(partition));
            var offsetMeta = committed.get(partition);

            if (offsetMeta == null) {
                // No committed offset — offset was lost
                log.warn("[OffsetRecovery] No committed offset for {} — applying {} fallback",
                        partition, fallback);

                switch (fallback) {
                    case TIMESTAMP -> seekByTimestamp(consumer, partition);
                    case EARLIEST -> {
                        consumer.seekToBeginning(java.util.List.of(partition));
                        log.info("[OffsetRecovery] Seeked to EARLIEST for {}", partition);
                    }
                    case LATEST -> {
                        consumer.seekToEnd(java.util.List.of(partition));
                        log.info("[OffsetRecovery] Seeked to LATEST for {}", partition);
                    }
                }
            }
        }
    }

    private void seekByTimestamp(Consumer<?, ?> consumer, TopicPartition partition) {
        long targetTimestamp = Instant.now()
                .minus(Duration.ofHours(fallbackHours))
                .toEpochMilli();

        Map<TopicPartition, Long> timestamps = new HashMap<>();
        timestamps.put(partition, targetTimestamp);

        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestamps);
        OffsetAndTimestamp offsetAndTimestamp = result.get(partition);

        if (offsetAndTimestamp != null) {
            consumer.seek(partition, offsetAndTimestamp.offset());
            log.info("[OffsetRecovery] Seeked {} to offset {} (timestamp: {} — {}h ago)",
                    partition, offsetAndTimestamp.offset(),
                    Instant.ofEpochMilli(offsetAndTimestamp.timestamp()),
                    fallbackHours);
        } else {
            // No messages at that timestamp — topic may be empty or all messages are older
            // Fall back to end (latest) to avoid replaying ancient messages
            consumer.seekToEnd(java.util.List.of(partition));
            log.warn("[OffsetRecovery] No messages found at {}h ago for {} — seeked to END",
                    fallbackHours, partition);
        }
    }
}
