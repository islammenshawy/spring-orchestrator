package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a @Step method for parallel execution within a named group.
 * Steps with the same group name and same order execute concurrently.
 * Use @JoinOn to wait for all steps in a group to complete.
 *
 * <pre>
 * @Step(order = 2)
 * @Parallel(group = "prep")
 * public void uploadAttachment(MyFlow flow) { ... }
 *
 * @Step(order = 2)
 * @Parallel(group = "prep")
 * public void requestSignature(MyFlow flow) { ... }
 *
 * @Step(order = 3)
 * @JoinOn(group = "prep")  // waits for both to complete
 * public void verifyBoth(MyFlow flow) { ... }
 * </pre>
 *
 * Under the hood: the orchestrator publishes one Kafka message per
 * parallel step. Each runs independently with its own retry/idempotency.
 * The join step only executes when all parallel steps are in the
 * flow's completedSteps set.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Parallel {

    /** Group name. Steps with the same group execute concurrently. */
    String group();
}
