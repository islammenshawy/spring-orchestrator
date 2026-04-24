package com.orchestrator.starter.outbox;

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
@Document(collection = "orchestrator_outbox")
public class OutboxEvent {

    @Id
    private String id;

    private String flowId;
    private String topic;
    private String key;
    private String payload;

    @Indexed
    @Builder.Default
    private boolean published = false;

    private Instant publishedAt;

    @Builder.Default
    private Instant createdAt = Instant.now();
}
