package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as the cancellation handler for a @Step.
 *
 * Called when a flow is explicitly cancelled via POST /flows/{id}/cancel.
 * Unlike @Compensate (which runs on permanent failure), @OnCancel runs
 * on deliberate user/system cancellation and may perform different cleanup.
 *
 * If @OnCancel is not defined for a step but @Compensate is, the library
 * falls back to @Compensate for that step during cancellation.
 *
 * Example:
 * <pre>
 * @Step(order = 2, completedWhen = "traceOriginalId != null")
 * public void registerDocument(MyFlow flow) {
 *     var res = enigioClient.createDocument(...);
 *     flow.setTraceOriginalId(res.traceOriginalId());
 * }
 *
 * @OnCancel(step = "registerDocument")
 * public void cancelDocument(MyFlow flow) {
 *     // Invalidate the document on Enigio
 *     enigioClient.invalidateDocument(flow.getTraceOriginalId());
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OnCancel {
    /** Name of the @Step method this cancellation handler is for. */
    String step();
}
