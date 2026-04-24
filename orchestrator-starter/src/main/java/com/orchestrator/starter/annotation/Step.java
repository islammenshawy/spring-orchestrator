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
 * @Step(order = 1, completedWhen = "documentId != null")
 * @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 * public void createDocument(MyFlow flow) {
 *     var res = vendorClient.createDocument(...);
 *     flow.setDocumentId(res.getId());
 * }
 *
 * @Step(order = 2, type = StepType.DB_WRITE)
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
     * SpEL expression evaluated against the flow object.
     * If true, the step is already completed — skip execution.
     * Empty = always execute (e.g., for stateless checks like signature verification).
     *
     * Examples:
     *   "documentId != null"
     *   "status == 'VERIFIED'"
     */
    String completedWhen() default "";

    /** Step type — determines what protection the library applies. */
    StepType type() default StepType.API_CALL;
}
