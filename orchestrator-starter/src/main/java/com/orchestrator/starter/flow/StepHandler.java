package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;

/**
 * Interface for a single step in a workflow.
 * Implement one per step. The orchestrator calls them in order.
 *
 * Usage:
 * <pre>
 * @Component
 * public class CreateDocumentStep implements StepHandler&lt;MyFlow&gt; {
 *
 *     public String getStepName() { return "CREATE_DOCUMENT"; }
 *     public int getOrder() { return 1; }
 *
 *     public boolean isAlreadyCompleted(MyFlow flow) {
 *         return flow.getDocumentId() != null;
 *     }
 *
 *     public void execute(MyFlow flow) {
 *         var response = vendorClient.createDocument(...);
 *         flow.setDocumentId(response.getId());
 *     }
 * }
 * </pre>
 *
 * @param <F> the flow entity type (must extend OrchestratorFlow)
 */
public interface StepHandler<F extends OrchestratorFlow> {

    /**
     * Unique name for this step (e.g., "CREATE_DOCUMENT").
     */
    String getStepName();

    /**
     * Execution order. Steps run in ascending order (1, 2, 3, ...).
     */
    int getOrder();

    /**
     * Idempotency guard — return true if this step's result is already
     * persisted on the flow (e.g., documentId is already set).
     * If true, execute() will not be called on redelivery.
     */
    boolean isAlreadyCompleted(F flow);

    /**
     * Execute the step. Modify the flow object with results.
     * The orchestrator persists the flow after this returns.
     *
     * @throws com.orchestrator.starter.exception.RetryableStepException
     *         for transient failures (vendor 500, timeout) — will be retried
     *         via Kafka retry topics with exponential backoff + jitter
     * @throws com.orchestrator.starter.exception.NonRetryableStepException
     *         for permanent failures — flow marked FAILED immediately
     */
    void execute(F flow);
}
