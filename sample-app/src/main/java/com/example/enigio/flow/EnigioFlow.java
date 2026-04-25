package com.example.enigio.flow;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Domain-specific flow entity for Enigio integration.
 * Implements OrchestratorFlow for the starter's generic engine.
 * Add your own fields — the starter only uses the OrchestratorFlow interface.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "enigio_flows")
public class EnigioFlow implements OrchestratorFlow {

    // === Required by OrchestratorFlow ===
    @Id
    private String id;

    @Indexed(unique = true)
    private String correlationId;

    private String currentStep;

    @Builder.Default
    private FlowStatus status = FlowStatus.PENDING;

    @Builder.Default
    private int retryCount = 0;

    @Builder.Default
    private int backoffSeconds = 0;

    private Instant nextRetryAt;
    private String errorMessage;

    @Builder.Default
    private Instant updatedAt = Instant.now();

    @Version
    private Long version;

    // Parallel step tracking (managed by library)
    @Builder.Default
    private java.util.Set<String> completedParallelSteps = new java.util.HashSet<>();

    // === Your domain fields ===
    private String title;
    private String content;
    private String signerEmail;

    // Results populated by step handlers
    private String enigioDocumentId;
    private String attachmentId;
    private String signatureRequestId;
    private String finalDocumentUrl;
    private String traceHash;

    @Builder.Default
    private Instant createdAt = Instant.now();
}
