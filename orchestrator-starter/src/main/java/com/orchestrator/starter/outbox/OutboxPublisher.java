package com.orchestrator.starter.outbox;

import lombok.RequiredArgsConstructor;
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
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${orchestrator.outbox.poll-interval-ms:500}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = repository.findTop100ByPublishedFalseOrderByCreatedAtAsc();

        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload()).get();
                event.setPublished(true);
                event.setPublishedAt(Instant.now());
                repository.save(event);
                log.debug("[Outbox] Published event {} to {}", event.getId(), event.getTopic());
            } catch (Exception e) {
                log.error("[Outbox] Failed to publish event {}: {}", event.getId(), e.getMessage());
                break; // Preserve ordering
            }
        }
    }
}
