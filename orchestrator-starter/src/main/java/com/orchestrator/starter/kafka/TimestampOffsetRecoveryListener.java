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
import java.util.List;
import java.util.Map;

/**
 * Handles offset recovery when committed offsets are lost.
 *
 * Primary use case: multi-cluster DC failover.
 * When failing over from DC-1 (Cluster A) to DC-2 (Cluster B),
 * messages are replicated via MirrorMaker/Cluster Linking but
 * consumer offsets are NOT — they live in __consumer_offsets which
 * is per-cluster.
 *
 * Without this listener:
 *   - auto.offset.reset=earliest → replay entire topic history
 *     → vendors flooded with stale requests (409/404)
 *   - auto.offset.reset=latest → skip everything
 *     → in-flight flows stuck forever
 *
 * With this listener:
 *   - Detects missing offsets on partition assignment
 *   - Uses offsetsForTimes() to seek to (now - N hours)
 *   - Only replays recent messages (likely still in-flight)
 *   - Idempotency guards handle any duplicates
 *
 * Execution order:
 *   1. Kafka assigns partition to consumer
 *   2. THIS listener runs (onPartitionsAssigned)
 *   3. consumer.seek() positions the read offset
 *   4. consumer.poll() starts reading from seeked position
 *   5. auto.offset.reset is NEVER reached (seek overrides it)
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
                log.warn("[OffsetRecovery] No committed offset for {} — applying {} fallback ({}h window)",
                        partition, fallback, fallbackHours);

                switch (fallback) {
                    case TIMESTAMP -> seekByTimestamp(consumer, partition);
                    case EARLIEST -> {
                        consumer.seekToBeginning(List.of(partition));
                        log.info("[OffsetRecovery] Seeked to EARLIEST for {}", partition);
                    }
                    case LATEST -> {
                        consumer.seekToEnd(List.of(partition));
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

        try {
            Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestamps);
            OffsetAndTimestamp offsetAndTimestamp = result.get(partition);

            if (offsetAndTimestamp != null) {
                consumer.seek(partition, offsetAndTimestamp.offset());
                log.info("[OffsetRecovery] {} → offset {} ({}h ago, timestamp={})",
                        partition, offsetAndTimestamp.offset(), fallbackHours,
                        Instant.ofEpochMilli(offsetAndTimestamp.timestamp()));
            } else {
                // No messages in the lookback window — topic empty or all messages older
                // Seek to end: no in-flight flows in this window anyway
                consumer.seekToEnd(List.of(partition));
                log.warn("[OffsetRecovery] No messages in last {}h for {} — seeked to END",
                        fallbackHours, partition);
            }
        } catch (Exception e) {
            // offsetsForTimes() failed — fall back to end (safe, stale flow recovery
            // scanner will catch stuck flows via MongoDB)
            log.error("[OffsetRecovery] offsetsForTimes() failed for {} — seeked to END: {}",
                    partition, e.getMessage());
            consumer.seekToEnd(List.of(partition));
        }
    }
}
