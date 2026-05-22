package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as the compensation (rollback) for a @Step.
 * Called in reverse order when a later step fails permanently.
 *
 * Usage:
 * <pre>
 * @Step(order = 1)
 * public void createDocument(MyFlow flow) {
 *     flow.setDocumentId(vendor.create(...));
 * }
 *
 * @Compensate(step = "createDocument")
 * public void undoCreateDocument(MyFlow flow) {
 *     vendor.delete(flow.getDocumentId());
 *     flow.setDocumentId(null);
 * }
 * </pre>
 *
 * If no @Compensate is defined for a step, the library logs a warning
 * and skips compensation for that step.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Compensate {

    /** Name of the @Step method this compensates. */
    String step();
}
