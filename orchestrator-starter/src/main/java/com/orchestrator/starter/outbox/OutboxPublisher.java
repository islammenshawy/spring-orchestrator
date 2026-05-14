package com.orchestrator.starter.outbox;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.List;

/**
 * Polls unpublished outbox events and sends to Kafka.
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
    private final OrchestratorMetrics metrics;

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate) {
        this(repository, kafkaTemplate, 5, null);
    }

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate,
                           int maxPublishRetries) {
        this(repository, kafkaTemplate, maxPublishRetries, null);
    }

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate kafkaTemplate,
                           int maxPublishRetries, OrchestratorMetrics metrics) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.maxPublishRetries = maxPublishRetries;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${orchestrator.outbox.poll-interval-ms:500}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = repository.findTop100ByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc();

        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload()).get();
                event.setPublished(true);
                event.setPublishedAt(Instant.now());
                event.setFailureCount(0);
                repository.save(event);
                if (metrics != null) metrics.outboxPublished();
                log.debug("[Outbox] Published event {} to {}", event.getId(), event.getTopic());
            } catch (Exception e) {
                int failures = event.getFailureCount() + 1;
                event.setFailureCount(failures);
                if (failures >= maxPublishRetries) {
                    event.setDeadLettered(true);
                    event.setPublishedAt(Instant.now()); // Enable TTL cleanup (same index as published events)
                    if (metrics != null) metrics.outboxDeadLettered();
                    log.error("[Outbox] Dead-lettering event {} after {} failures (flow: {}, topic: {}): {}",
                            event.getId(), failures, event.getFlowId(), event.getTopic(), e.getMessage());
                } else {
                    log.warn("[Outbox] Failed to publish event {} (attempt {}/{}): {}",
                            event.getId(), failures, maxPublishRetries, e.getMessage());
                }
                repository.save(event);
            }
        }
    }
}
