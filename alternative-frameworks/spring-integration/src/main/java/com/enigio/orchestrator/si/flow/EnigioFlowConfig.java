package com.enigio.orchestrator.si.flow;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.common.domain.FlowStep;
import com.enigio.orchestrator.common.exception.RetryableException;
import com.enigio.orchestrator.si.handler.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;

import java.time.Instant;

/**
 * Spring Integration flow with Kafka-backed retry.
 *
 * Each step:
 * 1. Calls Enigio API via handler
 * 2. On success: persists to MongoDB, publishes next step command to Kafka
 * 3. On retryable failure: throws RetryableException
 *    → Spring Kafka @RetryableTopic routes to retry-0 → retry-1 → retry-2 → DLT
 *    (same exponential backoff as Saga/SM patterns, crash-resilient)
 *
 * This gives us the declarative IntegrationFlow DSL for step definition
 * PLUS Kafka-durable retry that survives container crashes.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class EnigioFlowConfig {

    private final CreateDocumentHandler createDocumentHandler;
    private final UploadAttachmentHandler uploadAttachmentHandler;
    private final RequestSignatureHandler requestSignatureHandler;
    private final VerifySignatureHandler verifySignatureHandler;
    private final FinalizeDocumentHandler finalizeDocumentHandler;
    private final DocumentFlowRepository flowRepository;

    @Bean
    public MessageChannel enigioInputChannel() {
        return new DirectChannel();
    }

    /**
     * The IntegrationFlow handles one step at a time per Kafka message.
     * The flow reads the flow's currentStep and executes only that step.
     * On success it advances currentStep and publishes the next command to Kafka.
     * On failure it throws — Spring Kafka retry topics handle the backoff.
     */
    @Bean
    public IntegrationFlow enigioStepFlow() {
        return IntegrationFlow.from(enigioInputChannel())
                .handle(DocumentFlow.class, (flow, headers) -> {
                    FlowStep step = flow.getCurrentStep();
                    log.info("[SI] Executing step {} for flow {}", step, flow.getId());

                    flow.setStatus(FlowStatus.IN_PROGRESS);

                    // Execute the current step (throws RetryableException on failure)
                    switch (step) {
                        case CREATE_DOCUMENT -> createDocumentHandler.handle(flow);
                        case UPLOAD_ATTACHMENT -> uploadAttachmentHandler.handle(flow);
                        case REQUEST_SIGNATURE -> requestSignatureHandler.handle(flow);
                        case VERIFY_SIGNATURE -> verifySignatureHandler.handle(flow);
                        case FINALIZE_DOCUMENT -> finalizeDocumentHandler.handle(flow);
                    }

                    // Persist step result
                    flow.setUpdatedAt(Instant.now());
                    flow.setRetryCount(0);
                    flow.setBackoffSeconds(0);
                    flow.setNextRetryAt(null);

                    FlowStep nextStep = step.next();
                    if (nextStep == null) {
                        flow.setStatus(FlowStatus.COMPLETED);
                        flowRepository.save(flow);
                        log.info("[SI] Flow {} completed successfully", flow.getId());
                    } else {
                        flow.setCurrentStep(nextStep);
                        flowRepository.save(flow);
                    }

                    // Return null to terminate the IntegrationFlow —
                    // KafkaInboundConfig handles publishing the next step command
                    return null;
                })
                .get();
    }
}
