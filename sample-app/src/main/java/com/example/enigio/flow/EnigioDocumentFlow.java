package com.example.enigio.flow;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

/**
 * Complete Enigio integration flow — 5 steps in one class.
 *
 * Class-level annotations set defaults for all steps.
 * Method-level annotations override per step.
 * completedWhen SpEL prevents duplicate API calls on redelivery.
 *
 * The library handles everything else: Kafka retry topics with jitter,
 * outbox for atomic persistence, idempotency, DLT, recovery.
 */
@Slf4j
@Component
@Flow(topic = "enigio.sample.commands")
@RetryOn(httpStatus = {500, 502, 503, 429})
@FailOn(httpStatus = {400, 403})
public class EnigioDocumentFlow extends FlowDefinition<EnigioFlow> {

    private final WebClient vendorClient;

    public EnigioDocumentFlow(@Value("${vendor.base-url}") String baseUrl) {
        this.vendorClient = WebClient.create(baseUrl);
    }

    @Step(order = 1, completedWhen = "enigioDocumentId != null")
    @RecoverOn(httpStatus = 409, message = "already", action = RecoverAction.SKIP)
    public void createDocument(EnigioFlow flow) {
        log.info("Creating document for flow {}", flow.getId());
        Map res = vendorClient.post().uri("/documents")
                .bodyValue(Map.of("title", flow.getTitle(), "content", flow.getContent()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setEnigioDocumentId((String) res.get("documentId"));
    }

    @Step(order = 2, completedWhen = "attachmentId != null")
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    public void uploadAttachment(EnigioFlow flow) {
        log.info("Uploading attachment for flow {}", flow.getId());
        Map res = vendorClient.post().uri("/documents/{id}/attachments", flow.getEnigioDocumentId())
                .bodyValue(Map.of("fileName", "doc.pdf", "fileContent", flow.getContent()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setAttachmentId((String) res.get("attachmentId"));
    }

    @Step(order = 3, completedWhen = "signatureRequestId != null")
    @RecoverOn(httpStatus = 409, message = "already signed", action = RecoverAction.SKIP)
    public void requestSignature(EnigioFlow flow) {
        log.info("Requesting signature for flow {}", flow.getId());
        Map res = vendorClient.post().uri("/documents/{id}/sign", flow.getEnigioDocumentId())
                .bodyValue(Map.of("signerEmail", flow.getSignerEmail()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setSignatureRequestId((String) res.get("signatureRequestId"));
    }

    @Step(order = 4)
    public void verifySignature(EnigioFlow flow) {
        log.info("Verifying signature for flow {}", flow.getId());
        Map res = vendorClient.get()
                .uri("/documents/{id}/signature-status?signatureRequestId={sid}",
                        flow.getEnigioDocumentId(), flow.getSignatureRequestId())
                .retrieve().bodyToMono(Map.class).block();
        if (!Boolean.TRUE.equals(res.get("verified"))) {
            throw new RetryableStepException("Signature not yet verified");
        }
    }

    @Step(order = 5, completedWhen = "finalDocumentUrl != null")
    @RecoverOn(httpStatus = 409, message = "already finalized", action = RecoverAction.SKIP)
    public void finalizeDocument(EnigioFlow flow) {
        log.info("Finalizing document for flow {}", flow.getId());
        Map res = vendorClient.post().uri("/documents/{id}/finalize", flow.getEnigioDocumentId())
                .retrieve().bodyToMono(Map.class).block();
        flow.setFinalDocumentUrl((String) res.get("finalDocumentUrl"));
        flow.setTraceHash((String) res.get("traceHash"));
    }
}
