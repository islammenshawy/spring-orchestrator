package com.orchestrator.starter.outbox;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Polls unpublished outbox events and sends to Kafka in parallel batches.
 *
 * At-least-once delivery guarantee:
 * - If crash before send: event stays unpublished, retried next poll.
 * - If crash after send but before marking published: event re-sent,
 *   consumer idempotency deduplicates.
 * - If crash after marking published: clean.
 *
 * Poison event protection: events that fail to publish after maxPublishRetries
 * are marked as dead-lettered and skipped, preventing pipeline freezes.
 */
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository repository;
    private final KafkaTemplate kafkaTemplate;
    private final int maxPublishRetries;
    private final int batchSize;
    private final OrchestratorMetrics metrics;

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate) {
        this(repository, kafkaTemplate, 5, 5, null);
    }

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate,
                           int maxPublishRetries) {
        this(repository, kafkaTemplate, maxPublishRetries, 5, null);
    }

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate,
                           int maxPublishRetries, int batchSize, OrchestratorMetrics metrics) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.maxPublishRetries = maxPublishRetries;
        this.batchSize = batchSize;
        this.metrics = metrics != null ? metrics : OrchestratorMetrics.noop();
    }

    @SuppressWarnings("unchecked")
    @Scheduled(fixedDelayString = "${orchestrator.outbox.poll-interval-ms:500}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = repository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(
                PageRequest.of(0, batchSize));
        if (events.isEmpty()) return;

        // Fire all Kafka sends in parallel
        List<CompletableFuture<Void>> futures = new ArrayList<>(events.size());
        for (OutboxEvent event : events) {
            @SuppressWarnings("unchecked")
            CompletableFuture<Object> sendFuture =
                    (CompletableFuture<Object>) kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload());
            CompletableFuture<Void> future = sendFuture
                    .thenAccept(result -> {
                        event.setPublished(true);
                        event.setPublishedAt(Instant.now());
                        event.setFailureCount(0);
                        metrics.outboxPublished();
                        log.debug("[Outbox] Published event {} to {}", event.getId(), event.getTopic());
                    })
                    .exceptionally(ex -> {
                        int failures = event.getFailureCount() + 1;
                        event.setFailureCount(failures);
                        if (failures >= maxPublishRetries) {
                            event.setDeadLettered(true);
                            event.setPublishedAt(Instant.now());
                            metrics.outboxDeadLettered();
                            log.error("[Outbox] Dead-lettering event {} after {} failures (flow: {}, topic: {}): {}",
                                    event.getId(), failures, event.getFlowId(), event.getTopic(), ex.getMessage());
                        } else {
                            log.warn("[Outbox] Failed to publish event {} (attempt {}/{}): {}",
                                    event.getId(), failures, maxPublishRetries, ex.getMessage());
                        }
                        return null;
                    });
            futures.add(future);
        }

        // Wait for all sends to complete (with timeout to prevent indefinite blocking)
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[Outbox] Batch wait interrupted or timed out: {}", e.getMessage());
        }

        // Batch save all events in one MongoDB round-trip
        repository.saveAll(events);
    }
}
