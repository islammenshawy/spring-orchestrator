package com.enigio.orchestrator.saga.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Polls unpublished outbox events and sends to Kafka.
 *
 * Crash scenarios:
 * - Crash before kafkaTemplate.send(): no side effects, retry on next poll.
 * - Crash after send() but before marking published: message will be re-sent
 *   on next poll → consumer idempotency handles the duplicate.
 * - Crash after marking published: clean, no issues.
 *
 * The outbox pattern guarantees at-least-once delivery.
 * Consumer-side idempotency (via processed_events) provides effectively-once.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${outbox.polling-interval-ms:500}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = repository.findTop50ByPublishedFalseOrderByCreatedAtAsc();

        for (OutboxEvent event : events) {
            try {
                // Send to Kafka (synchronous — waits for broker ack)
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload()).get();

                // Mark as published
                event.setPublished(true);
                event.setPublishedAt(Instant.now());
                repository.save(event);

                log.debug("Published outbox event {} to topic {}", event.getId(), event.getTopic());
            } catch (Exception e) {
                log.error("Failed to publish outbox event {}: {}", event.getId(), e.getMessage());
                break; // Preserve ordering — don't skip ahead
            }
        }
    }
}
