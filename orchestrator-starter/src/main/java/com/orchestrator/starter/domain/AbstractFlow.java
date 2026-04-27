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

    private Instant createdAt = Instant.now();
}
