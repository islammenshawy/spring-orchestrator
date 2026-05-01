package com.orchestrator.starter.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Instant;

/**
 * Stores Kafka consumer offsets in MongoDB instead of __consumer_offsets.
 *
 * Why:
 *   In a multi-cluster DC failover, __consumer_offsets is per-cluster and
 *   NOT replicated. When failing from Cluster A → Cluster B, the consumer
 *   has no committed offset on Cluster B. Additionally, offsets on Cluster B
 *   may not match Cluster A (different ordering, compaction).
 *
 * How:
 *   After each successfully processed message, we save the Kafka message
 *   headers (topic, partition, offset, eventId, timestamp) to MongoDB.
 *   MongoDB IS replicated cross-DC via replica set.
 *
 *   On failover to Cluster B:
 *   1. Consumer starts, no offset in __consumer_offsets
 *   2. MongoOffsetRecoveryListener reads last processed offset from MongoDB
 *   3. Uses the stored eventId + timestamp to find position in Cluster B:
 *      - First tries: offsetsForTimes(lastTimestamp) to seek by timestamp
 *      - Then verifies: scans forward until eventId matches
 *   4. Consumer resumes from exact position — zero replay, zero loss
 *
 * Collection: orchestrator_consumer_offsets (TTL: none, small table)
 *
 * Config:
 *   orchestrator.recovery.offset-store: MONGO (default) | KAFKA
 */
@Slf4j
@RequiredArgsConstructor
public class MongoOffsetStore {

    private static final String COLLECTION = "orchestrator_consumer_offsets";

    private final MongoTemplate mongoTemplate;

    /**
     * Record that a message was successfully processed.
     * Called after step execution + offset commit.
     */
    public void saveOffset(String consumerGroup, String topic, int partition,
                           long offset, String eventId, long messageTimestamp) {
        String id = consumerGroup + "|" + topic + "|" + partition;

        mongoTemplate.upsert(
                Query.query(Criteria.where("_id").is(id)),
                new Update()
                        .set("consumerGroup", consumerGroup)
                        .set("topic", topic)
                        .set("partition", partition)
                        .set("offset", offset)
                        .set("eventId", eventId)
                        .set("messageTimestamp", messageTimestamp)
                        .set("updatedAt", Instant.now()),
                COLLECTION
        );
    }

    /**
     * Get the last processed offset for a partition.
     * Returns null if no offset stored (first time or collection empty).
     */
    public StoredOffset getLastOffset(String consumerGroup, String topic, int partition) {
        String id = consumerGroup + "|" + topic + "|" + partition;
        return mongoTemplate.findById(id, StoredOffset.class, COLLECTION);
    }

    /**
     * Get the last processed offset by consumer group and topic (any partition).
     * Useful for single-partition topics.
     */
    public StoredOffset getLastOffsetForTopic(String consumerGroup, String topic) {
        Query query = Query.query(
                Criteria.where("consumerGroup").is(consumerGroup)
                        .and("topic").is(topic))
                .with(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "offset"))
                .limit(1);
        return mongoTemplate.findOne(query, StoredOffset.class, COLLECTION);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Document(collection = COLLECTION)
    public static class StoredOffset {
        @Id
        private String id;
        private String consumerGroup;
        private String topic;
        private int partition;
        private long offset;
        private String eventId;
        private long messageTimestamp;
        private Instant updatedAt;
    }
}
