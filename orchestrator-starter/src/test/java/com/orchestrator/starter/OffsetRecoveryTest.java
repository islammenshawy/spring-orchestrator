package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.OffsetFallback;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.RecoveryConfig;
import com.orchestrator.starter.kafka.MongoOffsetRecoveryListener;
import com.orchestrator.starter.kafka.MongoOffsetStore;
import com.orchestrator.starter.kafka.MongoOffsetStore.StoredOffset;
import com.orchestrator.starter.kafka.TimestampOffsetRecoveryListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link TimestampOffsetRecoveryListener} and {@link MongoOffsetRecoveryListener}.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("Offset Recovery Listeners")
class OffsetRecoveryTest {

    private Consumer<?, ?> consumer;

    @BeforeEach
    void setUp() {
        consumer = mock(Consumer.class);
    }

    // ========================================================================
    // TimestampOffsetRecoveryListener
    // ========================================================================

    @Nested
    @DisplayName("TimestampOffsetRecoveryListener")
    class TimestampRecoveryTests {

        private RecoveryConfig recoveryConfig;

        @BeforeEach
        void setUp() {
            recoveryConfig = new RecoveryConfig();
            recoveryConfig.setOffsetFallback(OffsetFallback.TIMESTAMP);
            recoveryConfig.setOffsetFallbackHours(24);
        }

        @Test
        @DisplayName("empty partition list does nothing")
        void emptyPartitions_noop() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            listener.onPartitionsAssigned(consumer, Collections.emptyList());
            verifyNoInteractions(consumer);
        }

        @Test
        @DisplayName("partition with committed offset is left untouched")
        void committedOffset_noRecovery() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, new OffsetAndMetadata(100L));
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer, never()).seek(any(), anyLong());
            verify(consumer, never()).seekToBeginning(any());
            verify(consumer, never()).seekToEnd(any());
        }

        @Test
        @DisplayName("TIMESTAMP fallback: seeks to offset found by offsetsForTimes")
        void timestampFallback_seeksToFoundOffset() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            long expectedOffset = 500L;
            long expectedTimestamp = Instant.now().minus(Duration.ofHours(12)).toEpochMilli();
            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, new OffsetAndTimestamp(expectedOffset, expectedTimestamp));
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seek(tp, expectedOffset);
        }

        @Test
        @DisplayName("TIMESTAMP fallback: seeks to end when no messages in lookback window")
        void timestampFallback_seeksToEndWhenNoMessages() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, null); // no messages in window
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seekToEnd(List.of(tp));
            verify(consumer, never()).seek(any(), anyLong());
        }

        @Test
        @DisplayName("TIMESTAMP fallback: seeks to end when offsetsForTimes throws exception")
        void timestampFallback_seeksToEndOnException() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            when(consumer.offsetsForTimes(anyMap())).thenThrow(new RuntimeException("Kafka unavailable"));

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seekToEnd(List.of(tp));
            verify(consumer, never()).seek(any(), anyLong());
        }

        @Test
        @DisplayName("EARLIEST fallback: seeks to beginning")
        void earliestFallback_seeksToBeginning() {
            recoveryConfig.setOffsetFallback(OffsetFallback.EARLIEST);
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seekToBeginning(List.of(tp));
        }

        @Test
        @DisplayName("LATEST fallback: seeks to end")
        void latestFallback_seeksToEnd() {
            recoveryConfig.setOffsetFallback(OffsetFallback.LATEST);
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seekToEnd(List.of(tp));
        }

        @Test
        @DisplayName("multiple partitions: only those without committed offsets get recovery")
        void multiplePartitions_selectiveRecovery() {
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp0 = new TopicPartition("my-topic", 0);
            TopicPartition tp1 = new TopicPartition("my-topic", 1);
            TopicPartition tp2 = new TopicPartition("my-topic", 2);

            // tp0 has committed offset, tp1 and tp2 do not
            Map<TopicPartition, OffsetAndMetadata> committed0 = new HashMap<>();
            committed0.put(tp0, new OffsetAndMetadata(200L));
            when(consumer.committed(Set.of(tp0))).thenReturn(committed0);

            Map<TopicPartition, OffsetAndMetadata> committed1 = new HashMap<>();
            committed1.put(tp1, null);
            when(consumer.committed(Set.of(tp1))).thenReturn(committed1);

            Map<TopicPartition, OffsetAndMetadata> committed2 = new HashMap<>();
            committed2.put(tp2, null);
            when(consumer.committed(Set.of(tp2))).thenReturn(committed2);

            // offsetsForTimes returns results for both uncommitted partitions
            when(consumer.offsetsForTimes(anyMap())).thenAnswer(inv -> {
                Map<TopicPartition, Long> query = inv.getArgument(0);
                Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                for (var entry : query.entrySet()) {
                    result.put(entry.getKey(), new OffsetAndTimestamp(300L, entry.getValue()));
                }
                return result;
            });

            listener.onPartitionsAssigned(consumer, List.of(tp0, tp1, tp2));

            // tp0 should NOT be seeked (has committed offset)
            verify(consumer, never()).seek(eq(tp0), anyLong());
            // tp1 and tp2 should be seeked
            verify(consumer).seek(eq(tp1), eq(300L));
            verify(consumer).seek(eq(tp2), eq(300L));
        }

        @Test
        @DisplayName("TIMESTAMP fallback uses configured fallback hours for lookback")
        void timestampFallback_usesConfiguredHours() {
            recoveryConfig.setOffsetFallbackHours(6);
            var listener = new TimestampOffsetRecoveryListener(recoveryConfig);
            TopicPartition tp = new TopicPartition("my-topic", 0);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, new OffsetAndTimestamp(100L, System.currentTimeMillis()));
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            long beforeCall = Instant.now().minus(Duration.ofHours(6)).toEpochMilli();
            listener.onPartitionsAssigned(consumer, List.of(tp));
            long afterCall = Instant.now().minus(Duration.ofHours(6)).toEpochMilli();

            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<TopicPartition, Long>> captor = ArgumentCaptor.forClass(Map.class);
            verify(consumer).offsetsForTimes(captor.capture());

            long requestedTs = captor.getValue().get(tp);
            // The requested timestamp should be approximately 6 hours ago (within a 2-second tolerance)
            assertThat(requestedTs).isBetween(beforeCall - 2000, afterCall + 2000);
        }
    }

    // ========================================================================
    // MongoOffsetRecoveryListener
    // ========================================================================

    @Nested
    @DisplayName("MongoOffsetRecoveryListener")
    class MongoRecoveryTests {

        private MongoOffsetStore offsetStore;
        private TimestampOffsetRecoveryListener fallbackListener;
        private MongoOffsetRecoveryListener listener;

        @BeforeEach
        void setUp() {
            offsetStore = mock(MongoOffsetStore.class);
            RecoveryConfig recoveryConfig = new RecoveryConfig();
            recoveryConfig.setOffsetFallback(OffsetFallback.TIMESTAMP);
            recoveryConfig.setOffsetFallbackHours(24);
            fallbackListener = spy(new TimestampOffsetRecoveryListener(recoveryConfig));
            listener = new MongoOffsetRecoveryListener(offsetStore, "test-group", fallbackListener);
        }

        @Test
        @DisplayName("empty partition list does nothing")
        void emptyPartitions_noop() {
            listener.onPartitionsAssigned(consumer, Collections.emptyList());
            verifyNoInteractions(offsetStore);
            verifyNoInteractions(consumer);
        }

        @Test
        @DisplayName("MongoDB offset ahead of Kafka: seeks using stored timestamp")
        void mongoAheadOfKafka_seeksViaTimestamp() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 500L, "evt-500", 1714500000000L, Instant.now());
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            // Kafka has stale offset (behind MongoDB)
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, new OffsetAndMetadata(100L));
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            // offsetsForTimes returns the correct position
            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, new OffsetAndTimestamp(480L, 1714500000000L));
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seek(tp, 480L);
            verify(fallbackListener, never()).onPartitionsAssigned(any(), any());
        }

        @Test
        @DisplayName("Kafka offset at or ahead of MongoDB: no recovery needed")
        void kafkaAtOrAheadOfMongo_noRecovery() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 100L, "evt-100", 1714500000000L, Instant.now());
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            // Kafka has offset at or ahead of MongoDB
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, new OffsetAndMetadata(200L));
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer, never()).seek(any(), anyLong());
            verify(consumer, never()).seekToBeginning(any());
            verify(consumer, never()).seekToEnd(any());
        }

        @Test
        @DisplayName("No Kafka offset but MongoDB has offset: recovers from MongoDB")
        void noKafkaOffset_mongoHasOffset_recoversFromMongo() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 300L, "evt-300", 1714500000000L, Instant.now());
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            // No Kafka offset
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            // offsetsForTimes finds the message
            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, new OffsetAndTimestamp(290L, 1714500000000L));
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seek(tp, 290L);
        }

        @Test
        @DisplayName("Neither Kafka nor MongoDB has offset: delegates to fallback listener")
        void neitherKafkaNorMongo_delegatesToFallback() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            // MongoDB has nothing
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(null);

            // Kafka has nothing
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(fallbackListener).onPartitionsAssigned(eq(consumer), argThat(partitions ->
                    partitions.size() == 1 && partitions.contains(tp)));
        }

        @Test
        @DisplayName("MongoDB unavailable: falls back to timestamp listener")
        void mongoUnavailable_fallsBackToTimestampListener() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            when(offsetStore.getLatestOffsetAcrossClusters(anyString(), anyString(), anyInt()))
                    .thenThrow(new RuntimeException("MongoDB connection refused"));

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(fallbackListener).onPartitionsAssigned(eq(consumer), argThat(partitions ->
                    partitions.size() == 1 && partitions.contains(tp)));
        }

        @Test
        @DisplayName("Stored timestamp too old (message expired): seeks to beginning")
        void storedTimestampTooOld_seeksToBeginning() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 10L, "evt-10", 1614500000000L, Instant.now()); // very old timestamp
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            // No Kafka offset
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            // offsetsForTimes returns null — timestamp too old, all messages deleted by retention
            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, null);
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            verify(consumer).seekToBeginning(List.of(tp));
        }

        @Test
        @DisplayName("offsetsForTimes fails during MongoDB recovery: delegates to fallback")
        void offsetsForTimesFails_delegatesToFallback() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 300L, "evt-300", 1714500000000L, Instant.now());
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            // No Kafka offset
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            // offsetsForTimes throws
            when(consumer.offsetsForTimes(anyMap())).thenThrow(new RuntimeException("Broker timeout"));

            listener.onPartitionsAssigned(consumer, List.of(tp));

            // Should delegate the failed partition to fallback
            verify(fallbackListener).onPartitionsAssigned(eq(consumer), argThat(partitions ->
                    partitions.size() == 1 && partitions.contains(tp)));
        }

        @Test
        @DisplayName("MongoDB has offset, Kafka has nothing: no redundant committed() check")
        void mongoHasOffset_noKafka_onlyOneCommittedCheck() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            StoredOffset stored = new StoredOffset("id", "dc-a", "test-group", "my-topic",
                    0, 300L, "evt-300", 1714500000000L, Instant.now());
            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(stored);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, null);
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            Map<TopicPartition, OffsetAndTimestamp> offsetsResult = new HashMap<>();
            offsetsResult.put(tp, new OffsetAndTimestamp(290L, 1714500000000L));
            when(consumer.offsetsForTimes(anyMap())).thenReturn(offsetsResult);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            // committed() is called once for the partition inside recoverFromMongoDB
            verify(consumer).committed(Set.of(tp));
            verify(consumer).seek(tp, 290L);
        }

        @Test
        @DisplayName("No MongoDB offset but Kafka has offset: trusts Kafka, no fallback")
        void noMongoOffset_kafkaHasOffset_trustsKafka() {
            TopicPartition tp = new TopicPartition("my-topic", 0);

            when(offsetStore.getLatestOffsetAcrossClusters("test-group", "my-topic", 0))
                    .thenReturn(null);

            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            committed.put(tp, new OffsetAndMetadata(150L));
            when(consumer.committed(Set.of(tp))).thenReturn(committed);

            listener.onPartitionsAssigned(consumer, List.of(tp));

            // No seek needed — Kafka offset is trusted
            verify(consumer, never()).seek(any(), anyLong());
            verify(consumer, never()).seekToBeginning(any());
            verify(consumer, never()).seekToEnd(any());
            verify(fallbackListener, never()).onPartitionsAssigned(any(), any());
        }
    }
}
