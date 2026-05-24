package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a signal handler on a @Flow class.
 * Signal handlers are invoked when an external system sends a signal
 * to a running flow via {@code FlowOrchestrator.signal()}.
 *
 * Unlike @Step methods (which execute in order), signal handlers
 * can fire at any point in the flow's lifecycle. They modify the
 * flow entity and the library re-activates the current step if
 * the flow is parked.
 *
 * <pre>
 * @Signal
 * public void approve(MyFlow flow, ApprovalData data) {
 *     flow.setApproved(true);
 *     flow.setApprovedBy(data.getApprovedBy());
 * }
 * </pre>
 *
 * The method must accept the flow entity as the first parameter.
 * An optional second parameter is the signal payload (deserialized
 * from the caller's data).
 *
 * Signal name defaults to the method name. Override with {@code name()}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Signal {

    /** Signal name. Defaults to method name if not set. */
    String name() default "";
}
