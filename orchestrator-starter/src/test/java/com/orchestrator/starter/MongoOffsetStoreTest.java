package com.orchestrator.starter;

import com.orchestrator.starter.kafka.MongoOffsetStore;
import com.orchestrator.starter.kafka.MongoOffsetStore.StoredOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link MongoOffsetStore} — stores/retrieves Kafka consumer offsets in MongoDB.
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("MongoOffsetStore")
class MongoOffsetStoreTest {

    private MongoTemplate mongoTemplate;

    @Nested
    @DisplayName("Single-cluster (default clusterId)")
    class SingleCluster {

        private MongoOffsetStore store;

        @BeforeEach
        void setUp() {
            mongoTemplate = mock(MongoTemplate.class);
            store = new MongoOffsetStore(mongoTemplate, "default");
        }

        @Test
        @DisplayName("saveOffset upserts with correct key format: group|topic|partition")
        void saveOffset_upsertsWithCorrectKey() {
            store.saveOffset("my-group", "my-topic", 3, 1500L, "evt-abc", 1714500000000L);

            ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(mongoTemplate).upsert(queryCaptor.capture(), updateCaptor.capture(), eq("orchestrator_consumer_offsets"));

            String queryStr = queryCaptor.getValue().toString();
            assertThat(queryStr).contains("my-group|my-topic|3");

            String updateStr = updateCaptor.getValue().toString();
            assertThat(updateStr).contains("my-group");
            assertThat(updateStr).contains("my-topic");
            assertThat(updateStr).contains("1500");
            assertThat(updateStr).contains("evt-abc");
            assertThat(updateStr).contains("1714500000000");
        }

        @Test
        @DisplayName("getLastOffset queries by composite key and returns stored offset")
        void getLastOffset_returnStoredOffset() {
            StoredOffset expected = new StoredOffset(
                    "my-group|my-topic|0", "default", "my-group", "my-topic",
                    0, 42L, "evt-1", 1714500000000L, Instant.now());
            when(mongoTemplate.findById("my-group|my-topic|0", StoredOffset.class, "orchestrator_consumer_offsets"))
                    .thenReturn(expected);

            StoredOffset result = store.getLastOffset("my-group", "my-topic", 0);

            assertThat(result).isNotNull();
            assertThat(result.getOffset()).isEqualTo(42L);
            assertThat(result.getEventId()).isEqualTo("evt-1");
        }

        @Test
        @DisplayName("getLastOffset returns null when no offset stored")
        void getLastOffset_returnsNullWhenNotFound() {
            when(mongoTemplate.findById(anyString(), eq(StoredOffset.class), anyString()))
                    .thenReturn(null);

            StoredOffset result = store.getLastOffset("unknown-group", "unknown-topic", 0);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("getLatestOffsetAcrossClusters queries by group+topic+partition, sorted by timestamp desc")
        void getLatestOffsetAcrossClusters_queriesSortedByTimestamp() {
            StoredOffset expected = new StoredOffset(
                    "dc-b|my-group|my-topic|0", "dc-b", "my-group", "my-topic",
                    0, 200L, "evt-latest", 1714600000000L, Instant.now());
            when(mongoTemplate.findOne(any(Query.class), eq(StoredOffset.class), eq("orchestrator_consumer_offsets")))
                    .thenReturn(expected);

            StoredOffset result = store.getLatestOffsetAcrossClusters("my-group", "my-topic", 0);

            assertThat(result).isNotNull();
            assertThat(result.getOffset()).isEqualTo(200L);
            assertThat(result.getClusterId()).isEqualTo("dc-b");

            ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
            verify(mongoTemplate).findOne(queryCaptor.capture(), eq(StoredOffset.class), eq("orchestrator_consumer_offsets"));
            String queryStr = queryCaptor.getValue().toString();
            assertThat(queryStr).contains("consumerGroup");
            assertThat(queryStr).contains("topic");
            assertThat(queryStr).contains("partition");
        }

        @Test
        @DisplayName("getLatestOffsetAcrossClusters returns null when no offsets exist")
        void getLatestOffsetAcrossClusters_returnsNullWhenEmpty() {
            when(mongoTemplate.findOne(any(Query.class), eq(StoredOffset.class), eq("orchestrator_consumer_offsets")))
                    .thenReturn(null);

            StoredOffset result = store.getLatestOffsetAcrossClusters("group", "topic", 0);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("getLastOffsetForTopic queries by group+topic sorted by offset desc")
        void getLastOffsetForTopic_queriesSortedByOffset() {
            StoredOffset expected = new StoredOffset(
                    "my-group|my-topic|2", "default", "my-group", "my-topic",
                    2, 999L, "evt-high", 1714500000000L, Instant.now());
            when(mongoTemplate.findOne(any(Query.class), eq(StoredOffset.class), eq("orchestrator_consumer_offsets")))
                    .thenReturn(expected);

            StoredOffset result = store.getLastOffsetForTopic("my-group", "my-topic");

            assertThat(result).isNotNull();
            assertThat(result.getOffset()).isEqualTo(999L);
            assertThat(result.getPartition()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Multi-cluster (custom clusterId)")
    class MultiCluster {

        private MongoOffsetStore store;

        @BeforeEach
        void setUp() {
            mongoTemplate = mock(MongoTemplate.class);
            store = new MongoOffsetStore(mongoTemplate, "dc-a");
        }

        @Test
        @DisplayName("saveOffset uses clusterId-prefixed key: dc-a|group|topic|partition")
        void saveOffset_clusterPrefixedKey() {
            store.saveOffset("my-group", "my-topic", 1, 500L, "evt-x", 1714500000000L);

            ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
            verify(mongoTemplate).upsert(queryCaptor.capture(), any(Update.class), eq("orchestrator_consumer_offsets"));

            String queryStr = queryCaptor.getValue().toString();
            assertThat(queryStr).contains("dc-a|my-group|my-topic|1");
        }

        @Test
        @DisplayName("getLastOffset uses clusterId-prefixed key")
        void getLastOffset_clusterPrefixedKey() {
            store.getLastOffset("my-group", "my-topic", 1);

            verify(mongoTemplate).findById("dc-a|my-group|my-topic|1", StoredOffset.class, "orchestrator_consumer_offsets");
        }
    }
}
