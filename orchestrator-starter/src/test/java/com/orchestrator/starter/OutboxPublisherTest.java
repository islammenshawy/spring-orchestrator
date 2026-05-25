package com.orchestrator.starter;

import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.outbox.OutboxPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class OutboxPublisherTest {

    private OutboxEventRepository repository;
    private KafkaTemplate kafkaTemplate;
    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        publisher = new OutboxPublisher(repository, kafkaTemplate, 3, 100, null); // max 3 retries, batch 100
    }

    @Test
    void publishPendingEvents_success_marksPublished() {
        OutboxEvent event = OutboxEvent.builder()
                .id("e1").flowId("f1").topic("topic").key("k1").payload("payload")
                .build();
        when(repository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(event));
        when(kafkaTemplate.send("topic", "k1", "payload"))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        assertTrue(event.isPublished());
        assertFalse(event.isDeadLettered());
        assertEquals(0, event.getFailureCount());
        verify(repository).saveAll(List.of(event));
    }

    @Test
    void publishPendingEvents_failure_incrementsCount_continuesNextEvent() {
        OutboxEvent poisonEvent = OutboxEvent.builder()
                .id("e1").flowId("f1").topic("topic").key("k1").payload("too-large")
                .build();
        OutboxEvent goodEvent = OutboxEvent.builder()
                .id("e2").flowId("f2").topic("topic").key("k2").payload("ok")
                .build();

        when(repository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(poisonEvent, goodEvent));
        when(kafkaTemplate.send("topic", "k1", "too-large"))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Message too large")));
        when(kafkaTemplate.send("topic", "k2", "ok"))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        // Poison event: failure count incremented, NOT dead-lettered yet (1 < 3)
        assertEquals(1, poisonEvent.getFailureCount());
        assertFalse(poisonEvent.isDeadLettered());
        assertFalse(poisonEvent.isPublished());

        // Good event: published successfully (no longer blocked by poison)
        assertTrue(goodEvent.isPublished());
        assertEquals(0, goodEvent.getFailureCount());

        // All events batch-saved in one call
        verify(repository).saveAll(List.of(poisonEvent, goodEvent));
    }

    @Test
    void publishPendingEvents_deadLetters_afterMaxRetries() {
        OutboxEvent poisonEvent = OutboxEvent.builder()
                .id("e1").flowId("f1").topic("topic").key("k1").payload("bad")
                .failureCount(2) // already failed twice, this will be attempt 3
                .build();

        when(repository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of(poisonEvent));
        when(kafkaTemplate.send("topic", "k1", "bad"))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Message too large")));

        publisher.publishPendingEvents();

        // After 3rd failure (>= maxPublishRetries=3): dead-lettered
        assertEquals(3, poisonEvent.getFailureCount());
        assertTrue(poisonEvent.isDeadLettered());
        verify(repository).saveAll(List.of(poisonEvent));
    }

    @Test
    void publishPendingEvents_emptyBatch_noOp() {
        when(repository.findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(any(Pageable.class)))
                .thenReturn(List.of());

        publisher.publishPendingEvents();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        verify(repository, never()).saveAll(anyList());
    }
}
