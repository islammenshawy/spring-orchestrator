package com.enigio.orchestrator.dashboard.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/data")
@RequiredArgsConstructor
public class DataGridController {

    private final MongoTemplate mongoTemplate;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ========== MongoDB ==========

    @GetMapping("/mongo/collections")
    public ResponseEntity<List<String>> getCollections() {
        Set<String> names = mongoTemplate.getCollectionNames();
        return ResponseEntity.ok(names.stream().sorted().collect(Collectors.toList()));
    }

    @GetMapping("/mongo/collections/{name}")
    public ResponseEntity<Map<String, Object>> getCollectionData(
            @PathVariable String name,
            @RequestParam(defaultValue = "0") int skip,
            @RequestParam(defaultValue = "50") int limit) {

        List<Document> docs = mongoTemplate.getCollection(name)
                .find()
                .sort(new Document("_id", -1))
                .skip(skip)
                .limit(limit)
                .into(new ArrayList<>());

        long count = mongoTemplate.getCollection(name).countDocuments();

        return ResponseEntity.ok(Map.of(
                "collection", name,
                "total", count,
                "skip", skip,
                "limit", limit,
                "documents", docs
        ));
    }

    @GetMapping("/mongo/collections/{name}/count")
    public ResponseEntity<Map<String, Object>> getCollectionCount(@PathVariable String name) {
        long count = mongoTemplate.getCollection(name).countDocuments();
        return ResponseEntity.ok(Map.of("collection", name, "count", count));
    }

    @DeleteMapping("/kafka/topics")
    public ResponseEntity<Map<String, Object>> deleteTopics(@RequestParam(defaultValue = "") String pattern) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            Set<String> topics = admin.listTopics().names().get();
            Set<String> toDelete = topics.stream()
                    .filter(t -> !t.startsWith("__") && (pattern.isEmpty() || t.contains(pattern)))
                    .collect(java.util.stream.Collectors.toSet());
            if (!toDelete.isEmpty()) {
                admin.deleteTopics(toDelete).all().get();
            }
            return ResponseEntity.ok(Map.of("deleted", toDelete));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ========== Kafka ==========

    @GetMapping("/kafka/topics")
    public ResponseEntity<List<Map<String, Object>>> getKafkaTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (AdminClient admin = AdminClient.create(props)) {
            ListTopicsResult topics = admin.listTopics();
            Set<String> topicNames = topics.names().get();

            Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames).allTopicNames().get();

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                TopicDescription desc = entry.getValue();
                result.add(Map.of(
                        "name", entry.getKey(),
                        "partitions", desc.partitions().size(),
                        "internal", desc.isInternal()
                ));
            }
            result.sort(Comparator.comparing(m -> (String) m.get("name")));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.ok(List.of(Map.of("error", e.getMessage())));
        }
    }

    /**
     * Get messages from a Kafka topic.
     * @param mode "all" = last N messages, "pending" = only unconsumed (after committed offset)
     * @param group consumer group to check committed offset (for mode=pending)
     */
    @GetMapping("/kafka/topics/{name}/messages")
    public ResponseEntity<Map<String, Object>> getTopicMessages(
            @PathVariable String name,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "pending") String mode,
            @RequestParam(defaultValue = "") String group) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dashboard-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<Map<String, Object>> messages = new ArrayList<>();
        Map<Integer, Long> committedOffsets = new java.util.HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(name).stream()
                    .map(pi -> new TopicPartition(name, pi.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);

            // Get committed offsets for the executor consumer group
            if ("pending".equals(mode)) {
                String consumerGroup = group.isEmpty() ? resolveConsumerGroup(name) : group;
                if (consumerGroup != null) {
                    try (var admin = org.apache.kafka.clients.admin.AdminClient.create(
                            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                        var offsets = admin.listConsumerGroupOffsets(consumerGroup)
                                .partitionsToOffsetAndMetadata().get(3, java.util.concurrent.TimeUnit.SECONDS);
                        for (var entry : offsets.entrySet()) {
                            if (entry.getKey().topic().equals(name)) {
                                committedOffsets.put(entry.getKey().partition(),
                                        entry.getValue().offset());
                            }
                        }
                    } catch (Exception e) {
                        // Fall back to showing all
                    }
                }
            }

            // Seek to start position
            if ("pending".equals(mode) && !committedOffsets.isEmpty()) {
                // Start from committed offset (only unconsumed messages)
                for (TopicPartition tp : partitions) {
                    long committed = committedOffsets.getOrDefault(tp.partition(), 0L);
                    consumer.seek(tp, committed);
                }
            } else {
                // Show last N messages
                consumer.seekToEnd(partitions);
                for (TopicPartition tp : partitions) {
                    long endOffset = consumer.position(tp);
                    long startOffset = Math.max(0, endOffset - limit);
                    consumer.seek(tp, startOffset);
                }
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> record : records) {
                long committed = committedOffsets.getOrDefault(record.partition(), -1L);
                messages.add(Map.of(
                        "partition", record.partition(),
                        "offset", record.offset(),
                        "key", record.key() != null ? record.key() : "",
                        "value", record.value() != null ? record.value() : "",
                        "timestamp", record.timestamp(),
                        "consumed", committed >= 0 && record.offset() < committed
                ));
            }
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("topic", name, "error", e.getMessage(), "messages", List.of()));
        }

        messages.sort((a, b) -> Long.compare((long) b.get("offset"), (long) a.get("offset")));

        return ResponseEntity.ok(Map.of(
                "topic", name,
                "count", messages.size(),
                "mode", mode,
                "committedOffsets", committedOffsets,
                "messages", messages
        ));
    }

    /** Resolve the likely consumer group for a topic based on naming convention */
    private String resolveConsumerGroup(String topicName) {
        // Try common group names used by the orchestrator
        String[] suffixes = {"-executor", "-orchestrator", "-dlt"};
        String baseName = topicName.contains("-retry") || topicName.contains("-dlt")
                ? topicName.substring(0, topicName.indexOf("-retry") > 0 ? topicName.indexOf("-retry") : topicName.indexOf("-dlt"))
                : topicName;

        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            var groups = admin.listConsumerGroups().all().get(3, java.util.concurrent.TimeUnit.SECONDS);
            for (var g : groups) {
                for (String suffix : suffixes) {
                    if (g.groupId().endsWith(suffix)) return g.groupId();
                }
            }
        } catch (Exception e) { /* ignore */ }
        return null;
    }
}
