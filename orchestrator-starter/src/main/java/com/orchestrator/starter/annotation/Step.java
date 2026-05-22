package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a step in a flow. Methods are discovered in the
 * enclosing @Flow class and executed in order.
 *
 * Can also be used at class level for the multi-class approach
 * (one StepHandler class per step).
 *
 * Usage:
 * <pre>
 * @Step(order = 1)
 * @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 * public void createDocument(MyFlow flow) {
 *     var res = vendorClient.createDocument(...);
 *     flow.setDocumentId(res.getId());
 * }
 *
 * @Step(order = 2)
 * public void saveAuditRecord(MyFlow flow) {
 *     auditRepo.save(new AuditRecord(flow));
 * }
 * </pre>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Step {

    /** Step name. Defaults to method name in UPPER_SNAKE_CASE if not set. */
    String name() default "";

    /** Execution order. Steps run in ascending order. */
    int order();

    /**
     * Gate step expiry. When a step throws WaitingStepException (or calls
     * waitUntil() which throws internally), the library tracks how long
     * it has been waiting. If this duration is exceeded, the flow is
     * automatically failed by StaleFlowRecoveryService.
     *
     * Format: number + unit. Supported units: h (hours), d (days).
     * Examples: "48h", "7d", "72h"
     * Empty = no expiry (default).
     */
    String expiresAfter() default "";
}
