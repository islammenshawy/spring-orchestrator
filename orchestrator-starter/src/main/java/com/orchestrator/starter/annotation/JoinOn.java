package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a @Step method as a join point — it only executes after all
 * @Parallel steps in the named group have completed.
 *
 * The orchestrator checks each parallel step's completedWhen condition.
 * If any parallel step hasn't completed, the join step is skipped and
 * the message is not acknowledged (it will be redelivered).
 *
 * <pre>
 * @Step(order = 3)
 * @JoinOn(group = "prep")
 * public void afterBothComplete(MyFlow flow) {
 *     // Only runs when all @Parallel("prep") steps are done
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinOn {

    /** Group name to join on. Must match a @Parallel group. */
    String group();
}
