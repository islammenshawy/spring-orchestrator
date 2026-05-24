package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.WaitingStepException;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Base class for single-class flow definitions.
 *
 * Provides {@link #checkpoint(OrchestratorFlow)} for saving flow state
 * mid-step — critical when combining an API call with a DB write in
 * the same step.
 *
 * <pre>
 * @Step(order = 1)
 * public void chargePayment(MyFlow flow) {
 *     // 1. Call vendor API
 *     var result = paymentClient.charge(flow.getAmount());
 *
 *     // 2. Save result immediately — if container crashes after this,
 *     //    the library's completedSteps set already has this step marked
 *     flow.setPaymentId(result.getId());
 *     checkpoint(flow);
 *
 *     // 3. Now safe to do additional DB writes
 *     auditRepo.save(new AuditRecord(flow.getId(), result.getId()));
 * }
 * </pre>
 *
 * Without checkpoint: crash between API call and library's flow save
 * loses the paymentId. On redelivery, the library skips the step
 * (completedSteps contains it), but domain fields may be lost.
 *
 * With checkpoint: paymentId is persisted immediately.
 *
 * @param <F> the flow entity type
 */
public abstract class FlowDefinition<F extends OrchestratorFlow> {

    @org.springframework.context.annotation.Lazy
    @Autowired(required = false)
    @SuppressWarnings("unchecked")
    private OrchestratorFlowRepository rawRepository;

    @org.springframework.context.annotation.Lazy
    @Autowired(required = false)
    private FlowTypeRegistry flowTypeRegistry;

    /**
     * Saves the flow's current state to MongoDB immediately.
     * Call this after setting result fields from an API call,
     * before doing additional work in the same step.
     *
     * This ensures that if the container crashes after the API call,
     * the result is already persisted. On redelivery, the library checks
     * completedSteps — preventing duplicate API calls.
     */
    @SuppressWarnings("unchecked")
    protected void checkpoint(F flow) {
        rawRepository.save(flow);
    }

    /**
     * Gate step — parks the flow until an external trigger wakes it.
     * No Kafka cycling; the flow sleeps in MongoDB until a webhook or
     * API call re-publishes the step command. Fails the flow if the
     * condition is not met within {@code expiry}.
     *
     * <pre>
     * waitUntil(() -> flow.isApproved(), Duration.ofHours(48));
     * </pre>
     */
    protected void waitUntil(BooleanSupplier condition, Duration expiry) {
        if (!condition.getAsBoolean()) {
            throw new WaitingStepException("Waiting for condition",
                    WaitingStepException.WaitMode.PARKED, null, expiry);
        }
    }

    /**
     * Polling step — re-checks the condition on a configurable interval.
     * The scheduler re-delivers the step when {@code pollInterval} elapses.
     * Fails the flow if the condition is not met within {@code expiry}.
     *
     * <pre>
     * pollUntil(() -> "COMPLETED".equals(flow.getSigningStatus()),
     *           Duration.ofSeconds(30), Duration.ofHours(72));
     * </pre>
     */
    protected void pollUntil(BooleanSupplier condition, Duration pollInterval, Duration expiry) {
        if (!condition.getAsBoolean()) {
            throw new WaitingStepException("Waiting for condition",
                    WaitingStepException.WaitMode.POLLING, pollInterval, expiry);
        }
    }

    /**
     * Durable sleep — parks the flow for the given duration, then continues.
     * Survives container restarts. The scheduler wakes the flow when the
     * time elapses.
     *
     * <pre>
     * sleep(flow, Duration.ofHours(1));
     * // code here runs after 1 hour
     * </pre>
     */
    protected void sleep(F flow, Duration duration) {
        sleepUntil(flow, Instant.now().plus(duration));
    }

    /**
     * Durable sleep — parks the flow until the given instant, then continues.
     *
     * <pre>
     * sleepUntil(flow, Instant.parse("2026-06-01T00:00:00Z"));
     * </pre>
     */
    protected void sleepUntil(F flow, Instant wakeAt) {
        // On re-delivery after timer fires: sleep is done, continue
        if (flow.getSleepUntil() != null && !Instant.now().isBefore(flow.getSleepUntil())) {
            return;
        }
        // First call or timer hasn't fired yet: park
        flow.setSleepUntil(wakeAt);
        checkpoint(flow);
        Duration expiry = Duration.between(Instant.now(), wakeAt);
        throw new WaitingStepException("Sleeping until " + wakeAt,
                WaitingStepException.WaitMode.SLEEPING, null, expiry);
    }

    // ========== Child Workflows ==========

    /**
     * Blocking — starts a child flow and parks until it completes.
     *
     * <pre>
     * startChildFlow(flow, "enigio-instrument", child, Duration.ofHours(24));
     * // code here runs after child completes
     * </pre>
     *
     * @param flow       parent flow entity
     * @param childFlowType  flow type name the child runs through (e.g. "enigio-instrument")
     * @param child      child flow entity — set business fields only, library handles the rest
     * @param expiry     max time to wait for child completion
     * @return child flow ID
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected String startChildFlow(F flow, String childFlowType, OrchestratorFlow child, Duration expiry) {
        String childId = startChildFlowAsync(flow, childFlowType, child, expiry);
        awaitChildren(flow, expiry);
        return childId;
    }

    /**
     * Async — starts a child flow without parking. Must call
     * {@link #awaitChildren} later.
     *
     * <pre>
     * startChildFlowAsync(flow, "enigio-instrument", child1, Duration.ofHours(24));
     * startChildFlowAsync(flow, "enigio-instrument", child2, Duration.ofHours(24));
     * awaitChildren(flow, Duration.ofHours(24));
     * </pre>
     *
     * Library auto-sets: correlationId, parentFlowId, parentFlowType, parentStepName.
     * User only sets business fields on the child entity.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected String startChildFlowAsync(F flow, String childFlowType, OrchestratorFlow child, Duration expiry) {
        if (flowTypeRegistry == null) {
            throw new IllegalStateException("FlowTypeRegistry not available — cannot start child flows");
        }

        // Idempotency: check if this child was already started (re-delivery)
        List<String> childIds = flow.getChildFlowIds();
        if (childIds == null) {
            childIds = new ArrayList<>();
            flow.setChildFlowIds(childIds);
        }

        // Auto-generate deterministic correlation ID for the child.
        // If user set a correlationId (business key), prefix with parent context.
        // If not set, use index — but user SHOULD set it for crash-safe idempotency.
        if (child.getCorrelationId() == null) {
            child.setCorrelationId(flow.getId() + ":child:" + childFlowType + ":" + childIds.size());
        } else if (!child.getCorrelationId().startsWith(flow.getId())) {
            child.setCorrelationId(flow.getId() + ":child:" + child.getCorrelationId());
        }

        // Check if child with this correlationId already exists (re-delivery idempotency)
        FlowTypeDescriptor childDesc = flowTypeRegistry.resolve(childFlowType);
        var existingChild = childDesc.getRepository().findByCorrelationId(child.getCorrelationId());
        if (existingChild.isPresent()) {
            String existingId = ((OrchestratorFlow) existingChild.get()).getId();
            if (!childIds.contains(existingId)) {
                childIds.add(existingId);
                flow.setChildFlowIds(childIds);
            }
            return existingId;
        }

        // Set parent references on child (library plumbing — user doesn't touch these)
        child.setParentFlowId(flow.getId());
        child.setParentFlowType(flow.getFlowType());
        child.setParentStepName(flow.getCurrentStep());

        // Start child flow
        FlowOrchestrator childOrch = (FlowOrchestrator) childDesc.getOrchestrator();
        OrchestratorFlow started = childOrch.startFlow(child);

        // Track child ID on parent
        childIds.add(started.getId());
        flow.setChildFlowIds(childIds);
        checkpoint(flow);

        return started.getId();
    }

    /**
     * Parks until ALL child flows have completed or failed.
     * Call after one or more {@link #startChildFlowAsync} calls.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void awaitChildren(F flow, Duration expiry) {
        List<String> childIds = flow.getChildFlowIds();
        if (childIds == null || childIds.isEmpty()) return;

        for (String childId : childIds) {
            for (FlowTypeDescriptor desc : flowTypeRegistry.getAll()) {
                var repo = desc.getRepository();
                if (repo == null) continue;
                var childOpt = repo.findById(childId);
                if (childOpt.isPresent()) {
                    OrchestratorFlow child = (OrchestratorFlow) childOpt.get();
                    FlowStatus childStatus = child.getStatus();
                    if (childStatus != FlowStatus.COMPLETED && childStatus != FlowStatus.FAILED
                            && childStatus != FlowStatus.CANCELLED) {
                        throw new WaitingStepException(
                                "Waiting for child " + childId + " (status: " + childStatus + ")",
                                WaitingStepException.WaitMode.PARKED, null, expiry);
                    }
                    break;
                }
            }
        }
    }
}
