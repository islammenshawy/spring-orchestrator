package com.orchestrator.starter.outbox;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "orchestrator_outbox")
@CompoundIndex(name = "unpublished_idx", def = "{'published': 1, 'createdAt': 1}")
public class OutboxEvent {

    @Id
    private String id;

    private String flowId;
    private String topic;
    private String key;
    private String payload;

    @Builder.Default
    private boolean published = false;

    /** True if the event was moved to dead-letter after exceeding max publish retries. */
    @Builder.Default
    private boolean deadLettered = false;

    /** Number of consecutive publish failures. Reset on successful publish. */
    @Builder.Default
    private int failureCount = 0;

    private Instant publishedAt;  // TTL index created programmatically by IndexInitializer

    @Builder.Default
    private Instant createdAt = Instant.now();
}
