package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.OrchestratorFlow;

/**
 * Interface for a single step in a workflow.
 *
 * Two ways to define step metadata:
 *
 * Option 1: Override getStepName() and getOrder()
 * <pre>
 * @Component
 * public class CreateDocumentStep implements StepHandler&lt;MyFlow&gt; {
 *     public String getStepName() { return "CREATE_DOCUMENT"; }
 *     public int getOrder() { return 1; }
 *     ...
 * }
 * </pre>
 *
 * Option 2: Use @Step annotation (less boilerplate)
 * <pre>
 * @Component
 * @Step(name = "CREATE_DOCUMENT", order = 1)
 * @RetryOn(httpStatus = {500, 502, 503, 429})
 * @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 * @FailOn(httpStatus = {400, 403})
 * public class CreateDocumentStep implements StepHandler&lt;MyFlow&gt; {
 *     // no getStepName/getOrder needed — read from annotation
 *
 *     public void execute(MyFlow flow) {
 *         // no try/catch needed — @RetryOn/@RecoverOn/@FailOn handle it
 *         var result = vendorClient.createDocument(...);
 *         flow.setDocumentId(result.getId());
 *     }
 * }
 * </pre>
 */
public interface StepHandler<F extends OrchestratorFlow> {

    /**
     * Step name. Default reads from @Step annotation if present.
     */
    default String getStepName() {
        Step step = this.getClass().getAnnotation(Step.class);
        if (step != null) return step.name();
        throw new IllegalStateException(
                this.getClass().getSimpleName() + ": override getStepName() or use @Step annotation");
    }

    /**
     * Execution order. Default reads from @Step annotation if present.
     */
    default int getOrder() {
        Step step = this.getClass().getAnnotation(Step.class);
        if (step != null) return step.order();
        throw new IllegalStateException(
                this.getClass().getSimpleName() + ": override getOrder() or use @Step annotation");
    }

    /**
     * Execute the step. Set results on the flow object.
     *
     * Error handling options (choose one):
     * 1. Annotations: @RetryOn, @RecoverOn, @FailOn on the class — no try/catch needed
     * 2. Manual: throw RetryableStepException or NonRetryableStepException
     * 3. Default: any unhandled exception is treated as retryable
     */
    void execute(F flow);

    /** Step expiry duration. Null = no expiry. */
    default java.time.Duration getExpiresAfter() { return null; }
}
