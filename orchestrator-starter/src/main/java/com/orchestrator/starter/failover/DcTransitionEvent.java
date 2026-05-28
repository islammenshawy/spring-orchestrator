package com.orchestrator.starter.failover;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Audit record for DC transitions. Stored in MongoDB for observability.
 */
@Data
@Builder
@Document(collection = "orchestrator_dc_transitions")
public class DcTransitionEvent {
    @Id
    private String id;
    private String consumerId;
    private String fromDc;
    private String toDc;
    private String reason;
    private DcState previousState;
    private DcState newState;
    private int consecutiveFailures;
    @Builder.Default
    private Instant timestamp = Instant.now();
}
