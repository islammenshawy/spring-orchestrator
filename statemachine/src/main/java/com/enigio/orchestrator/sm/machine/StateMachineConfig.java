package com.enigio.orchestrator.sm.machine;

import com.enigio.orchestrator.sm.actions.*;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachineFactory;
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineConfigurationConfigurer;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

import java.util.EnumSet;

/**
 * Simplified SM config for Kafka-driven step execution.
 * Each step is a separate Kafka message. The SM validates
 * the transition and runs the action. On success, the Kafka
 * consumer advances to the next step.
 *
 * States: INITIAL → CREATING → UPLOADING → SIGNING → VERIFYING → FINALIZING → COMPLETED
 * Each transition is triggered by a step-specific event from the Kafka consumer.
 */
@Configuration
@EnableStateMachineFactory
@RequiredArgsConstructor
public class StateMachineConfig extends EnumStateMachineConfigurerAdapter<DocumentFlowStates, DocumentFlowEvents> {

    private final CreateDocumentAction createDocumentAction;
    private final UploadAttachmentAction uploadAttachmentAction;
    private final RequestSignatureAction requestSignatureAction;
    private final VerifySignatureAction verifySignatureAction;
    private final FinalizeDocumentAction finalizeDocumentAction;

    @Override
    public void configure(StateMachineConfigurationConfigurer<DocumentFlowStates, DocumentFlowEvents> config) throws Exception {
        config.withConfiguration()
                .autoStartup(false);
    }

    @Override
    public void configure(StateMachineStateConfigurer<DocumentFlowStates, DocumentFlowEvents> states) throws Exception {
        states.withStates()
                .initial(DocumentFlowStates.INITIAL)
                .end(DocumentFlowStates.COMPLETED)
                .end(DocumentFlowStates.FAILED)
                .states(EnumSet.allOf(DocumentFlowStates.class));
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<DocumentFlowStates, DocumentFlowEvents> transitions) throws Exception {
        transitions
                // START → execute CREATE_DOCUMENT
                .withExternal()
                .source(DocumentFlowStates.INITIAL)
                .target(DocumentFlowStates.CREATING_DOCUMENT)
                .event(DocumentFlowEvents.START)
                .action(createDocumentAction)
                .and()

                // After CREATE_DOCUMENT success → execute UPLOAD_ATTACHMENT
                .withExternal()
                .source(DocumentFlowStates.CREATING_DOCUMENT)
                .target(DocumentFlowStates.UPLOADING_ATTACHMENT)
                .event(DocumentFlowEvents.DOCUMENT_CREATE_SUCCESS)
                .action(uploadAttachmentAction)
                .and()

                // After UPLOAD_ATTACHMENT success → execute REQUEST_SIGNATURE
                .withExternal()
                .source(DocumentFlowStates.UPLOADING_ATTACHMENT)
                .target(DocumentFlowStates.REQUESTING_SIGNATURE)
                .event(DocumentFlowEvents.UPLOAD_SUCCESS)
                .action(requestSignatureAction)
                .and()

                // After REQUEST_SIGNATURE success → execute VERIFY_SIGNATURE
                .withExternal()
                .source(DocumentFlowStates.REQUESTING_SIGNATURE)
                .target(DocumentFlowStates.VERIFYING_SIGNATURE)
                .event(DocumentFlowEvents.SIGN_REQUEST_SUCCESS)
                .action(verifySignatureAction)
                .and()

                // After VERIFY_SIGNATURE success → execute FINALIZE_DOCUMENT
                .withExternal()
                .source(DocumentFlowStates.VERIFYING_SIGNATURE)
                .target(DocumentFlowStates.FINALIZING)
                .event(DocumentFlowEvents.VERIFY_SUCCESS)
                .action(finalizeDocumentAction)
                .and()

                // After FINALIZE_DOCUMENT success → COMPLETED
                .withExternal()
                .source(DocumentFlowStates.FINALIZING)
                .target(DocumentFlowStates.COMPLETED)
                .event(DocumentFlowEvents.FINALIZE_SUCCESS);
    }
}
