package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a step handler. Alternative to implementing StepHandler interface.
 * The library discovers annotated classes at startup and registers them in the step registry.
 *
 * Usage:
 * <pre>
 * @Component
 * @Step(name = "CREATE_DOCUMENT", order = 1)
 * @RetryOn(httpStatus = {500, 502, 503, 429})
 * @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 * public class CreateDocumentStep implements StepHandler&lt;MyFlow&gt; { ... }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Step {

    /** Step name (e.g., "CREATE_DOCUMENT") */
    String name();

    /** Execution order. Steps run in ascending order. */
    int order();
}
