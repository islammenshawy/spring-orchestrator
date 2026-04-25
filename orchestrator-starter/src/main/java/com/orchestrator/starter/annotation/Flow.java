package com.orchestrator.starter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a flow definition. All @Step-annotated methods
 * in this class form the steps of the workflow.
 *
 * Class-level @RetryOn, @RecoverOn, @FailOn set defaults for all steps.
 * Method-level annotations override the class defaults.
 *
 * Usage:
 * <pre>
 * @Component
 * @Flow(topic = "enigio.commands")
 * @RetryOn(httpStatus = {500, 502, 503, 429})
 * @FailOn(httpStatus = {400, 403})
 * public class EnigioDocumentFlow extends FlowDefinition&lt;EnigioFlow&gt; {
 *
 *     @Step(order = 1, completedWhen = "documentId != null")
 *     @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
 *     public void createDocument(EnigioFlow flow) { ... }
 *
 *     @Step(order = 2)
 *     public void saveAuditRecord(EnigioFlow flow) { ... }
 *
 *     @Step(order = 3, completedWhen = "signatureId != null")
 *     public void requestSignature(EnigioFlow flow) { ... }
 * }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Flow {

    /** Kafka topic for step commands. Overrides orchestrator.kafka.command-topic. */
    String topic() default "";
}
