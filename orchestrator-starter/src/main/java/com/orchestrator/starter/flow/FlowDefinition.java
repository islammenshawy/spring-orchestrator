package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Base class for single-class flow definitions.
 *
 * Provides {@link #checkpoint(OrchestratorFlow)} for saving flow state
 * mid-step — critical when combining an API call with a DB write in
 * the same step.
 *
 * <pre>
 * @Step(order = 1, completedWhen = "paymentId != null")
 * public void chargePayment(MyFlow flow) {
 *     // 1. Call vendor API
 *     var result = paymentClient.charge(flow.getAmount());
 *
 *     // 2. Save result immediately — if container crashes after this,
 *     //    redelivery sees paymentId is set, skips the API call
 *     flow.setPaymentId(result.getId());
 *     checkpoint(flow);
 *
 *     // 3. Now safe to do additional DB writes
 *     auditRepo.save(new AuditRecord(flow.getId(), result.getId()));
 * }
 * </pre>
 *
 * Without checkpoint: crash between API call and library's flow save
 * loses the paymentId. On redelivery, completedWhen is false, API is
 * called again (double charge on non-idempotent APIs).
 *
 * With checkpoint: paymentId is persisted immediately. Redelivery sees
 * completedWhen = true, skips the API call entirely.
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
     * the result is already persisted and completedWhen will return true
     * on redelivery — preventing duplicate API calls.
     */
    @SuppressWarnings("unchecked")
    protected void checkpoint(F flow) {
        rawRepository.save(flow);
    }
}
