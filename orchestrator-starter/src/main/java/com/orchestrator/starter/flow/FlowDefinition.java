package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.WaitingStepException;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
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
}
