package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;

/**
 * Base class for single-class flow definitions.
 * Extend this and annotate methods with @Step.
 *
 * <pre>
 * @Component
 * @Flow(topic = "enigio.commands")
 * @RetryOn(httpStatus = {500, 502, 503, 429})
 * @FailOn(httpStatus = {400, 403})
 * public class EnigioDocumentFlow extends FlowDefinition&lt;EnigioFlow&gt; {
 *
 *     @Step(order = 1, completedWhen = "enigioDocumentId != null")
 *     public void createDocument(EnigioFlow flow) { ... }
 *
 *     @Step(order = 2)
 *     public void saveAudit(EnigioFlow flow) { ... }
 * }
 * </pre>
 *
 * @param <F> the flow entity type
 */
public abstract class FlowDefinition<F extends OrchestratorFlow> {
}
