package com.enigio.orchestrator.common.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "document_flows")
public class DocumentFlow {

    @Id
    private String id;

    @Indexed(unique = true)
    private String correlationId;

    private String title;
    private String content;
    private String signerEmail;
    private String metadata;

    // Which orchestration pattern created this flow
    private String pattern; // "saga" or "statemachine"

    // Backoff tracking
    @Builder.Default
    private int backoffSeconds = 0;

    private Instant nextRetryAt;

    // Results populated as steps complete
    private String enigioDocumentId;
    private String attachmentId;
    private String signatureRequestId;
    private String finalDocumentUrl;
    private String traceHash;

    // Tracking
    @Builder.Default
    private FlowStep currentStep = FlowStep.CREATE_DOCUMENT;

    @Builder.Default
    private FlowStatus status = FlowStatus.PENDING;

    private String errorMessage;

    @Builder.Default
    private int retryCount = 0;

    @Builder.Default
    private Instant createdAt = Instant.now();

    @Builder.Default
    private Instant updatedAt = Instant.now();

    @Version
    private Long version;
}
