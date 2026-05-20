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

    /** Flow type identifier for multi-flow routing. Set by the library at startFlow(). */
    default String getFlowType() { return null; }
    default void setFlowType(String flowType) {}

    /**
     * Tracks which steps in a parallel group have completed.
     * Managed by the library — users don't interact with this.
     * Default implementation returns empty set (for non-parallel flows).
     */
    default java.util.Set<String> getCompletedParallelSteps() { return java.util.Set.of(); }
    default void setCompletedParallelSteps(java.util.Set<String> steps) {}

    /** Number of stale recovery re-publishes. Reset on step success. */
    default int getRecoveryCount() { return 0; }
    default void setRecoveryCount(int count) {}

    /** Error from failed compensation handler (when status=COMPENSATION_FAILED). */
    default String getCompensationError() { return null; }
    default void setCompensationError(String error) {}

    /** When this step first entered WAITING_RETRY. Set by library, reset on advancement. */
    default Instant getWaitingSince() { return null; }
    default void setWaitingSince(Instant waitingSince) {}
}
