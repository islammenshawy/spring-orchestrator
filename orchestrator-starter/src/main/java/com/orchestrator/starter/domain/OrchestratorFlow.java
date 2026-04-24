package com.orchestrator.starter.domain;

import java.time.Instant;

/**
 * Base interface for flow entities. Users extend this with their own fields.
 *
 * The orchestrator uses these fields to track flow progress.
 * Users add their domain-specific fields (e.g., documentId, signerEmail).
 *
 * Usage:
 * <pre>
 * @Document(collection = "my_flows")
 * public class MyFlow implements OrchestratorFlow {
 *     @Id private String id;
 *     private String correlationId;
 *     private String currentStep;
 *     private FlowStatus status = FlowStatus.PENDING;
 *     private int retryCount;
 *     private int backoffSeconds;
 *     private Instant nextRetryAt;
 *     private String errorMessage;
 *     private Instant updatedAt = Instant.now();
 *
 *     // Your domain fields
 *     private String documentId;
 *     private String attachmentId;
 *     // ... getters/setters or Lombok
 * }
 * </pre>
 */
public interface OrchestratorFlow {

    String getId();

    String getCorrelationId();

    String getCurrentStep();
    void setCurrentStep(String step);

    FlowStatus getStatus();
    void setStatus(FlowStatus status);

    int getRetryCount();
    void setRetryCount(int count);

    int getBackoffSeconds();
    void setBackoffSeconds(int seconds);

    Instant getNextRetryAt();
    void setNextRetryAt(Instant nextRetryAt);

    String getErrorMessage();
    void setErrorMessage(String message);

    Instant getUpdatedAt();
    void setUpdatedAt(Instant updatedAt);
}
