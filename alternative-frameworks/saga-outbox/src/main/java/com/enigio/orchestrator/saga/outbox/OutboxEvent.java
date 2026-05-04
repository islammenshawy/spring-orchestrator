package com.enigio.orchestrator.saga.outbox;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String topic;
    private String payload;

    @Indexed
    @Builder.Default
    private boolean published = false;

    private Instant publishedAt;

    @Builder.Default
    private Instant createdAt = Instant.now();
}
