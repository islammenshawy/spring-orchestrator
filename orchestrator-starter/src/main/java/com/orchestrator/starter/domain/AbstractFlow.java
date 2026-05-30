package com.orchestrator.starter.domain;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for flow entities. Provides all library-required fields.
 * Extend and add only your domain fields.
 *
 * <pre>
 * @Document(collection = "order_flows")
 * @Data
 * public class OrderFlow extends AbstractFlow {
 *     private BigDecimal amount;
 *     private String paymentId;
 *     private String trackingNumber;
 * }
 * </pre>
 *
 * All orchestrator tracking fields (id, status, retryCount, etc.) are
 * inherited. You never declare them.
 */
@Data
@CompoundIndex(name = "status_updated_idx", def = "{'status': 1, 'updatedAt': 1}")
public abstract class AbstractFlow implements OrchestratorFlow {

    @Id
    private String id;

    @Indexed(unique = true)
    private String correlationId;

    /** Flow type for multi-flow routing. Set by the library at startFlow(). */
    @org.springframework.data.mongodb.core.index.Indexed
    private String flowType;

    private String currentStep;

    private FlowStatus status = FlowStatus.PENDING;

    private int retryCount = 0;

    private int backoffSeconds = 0;

    private Instant nextRetryAt;

    private String errorMessage;

    private Instant updatedAt = Instant.now();

    @Version
    private Long version;

    private Set<String> completedParallelSteps = new HashSet<>();

    private Set<String> completedSteps = new HashSet<>();

    /** Number of times stale recovery has re-published this flow. Reset on step success. */
    private int recoveryCount = 0;

    /** Number of poll cycles for the current pollUntil() step. Reset on step advancement. */
    private int pollCount = 0;

    /** Error message from a failed compensation handler (when status=COMPENSATION_FAILED). */
    private String compensationError;

    /** When the current step first entered WAITING_RETRY/PARKED. Set by library, reset on advancement. */
    private Instant waitingSince;

    /** Absolute deadline for the current waiting step. Set by waitUntil()/pollUntil() on first park. */
    private Instant expiresAt;

    /** Target wake time for durable sleep. Set by sleep()/sleepUntil(), cleared on step success. */
    private Instant sleepUntil;

    /** Queued signals waiting to execute between steps. Null/empty = none pending. */
    private java.util.List<PendingSignal> pendingSignals;

    /** IDs of child flows started from this flow. */
    private java.util.List<String> childFlowIds;

    /** Parent flow ID — set on child flows. Null for top-level flows. */
    private String parentFlowId;

    /** Parent flow type — for routing re-activation to correct orchestrator. */
    private String parentFlowType;

    /** Parent step name — which step to re-publish when child completes. */
    private String parentStepName;

    /** Step currently being executed. Set atomically before execution, cleared on completion.
     *  Prevents concurrent consumers (e.g. Kafka rebalance) from executing the same step. */
    private String executingStep;

    /** Pod/instance ID executing the current step. For diagnostics and stale claim detection. */
    private String executingPod;

    /** Pod ID that claimed this flow for recovery processing. Null = unclaimed. */
    private String claimedBy;

    /** When this flow was claimed. Used for orphan TTL cleanup. */
    private Instant claimedAt;

    private Instant createdAt = Instant.now();
}
