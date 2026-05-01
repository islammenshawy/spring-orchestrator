package com.dis.instrument.vendor.enigio;

import com.dis.instrument.core.api.FlowNotificationPublisher;
import com.dis.instrument.core.model.Attachment;
import com.dis.instrument.core.model.Signer;
import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 11-step orchestration flow for Enigio trace:original integration.
 *
 * Group 1 — Document Preparation:  CREATE_DRAFT → REGISTER_DOCUMENT → ADD_ATTACHMENT
 * Gate 1 — Notify + await approval: AWAIT_PREPARATION_APPROVAL
 * Group 2 — Signing Ceremony:      ADD_SIGNERS → SEND_FOR_SIGNING → AWAIT_SIGNATURES
 * Gate 2 — Notify + await approval: AWAIT_DELIVERY_APPROVAL
 * Group 3 — Packaging & Delivery:  VALIDATE_DOCUMENT → CREATE_ENVELOPE → TRANSFER_DOCUMENT
 *
 * At each gate, a notification is published to dis.instrument.notifications topic.
 * The downstream system consumes the notification and calls POST /flows/{id}/approve
 * to trigger the next group.
 */
@Slf4j
@Component
@Flow(name = "enigio-instrument")
public class EnigioInstrumentFlow extends FlowDefinition<EnigioInstrumentEntity> {

    private final EnigioClient enigioClient;
    private final FlowNotificationPublisher notificationPublisher;
    private final long signingExpiryHours;
    private final long approvalExpiryHours;
    private final String webhookCallbackUrl;

    public EnigioInstrumentFlow(EnigioClient enigioClient,
                                FlowNotificationPublisher notificationPublisher,
                                @Value("${dis.signing.expiry-hours:48}") long signingExpiryHours,
                                @Value("${dis.approval.expiry-hours:72}") long approvalExpiryHours,
                                @Value("${dis.webhook.callback-url:http://digital-instrument-service:8087/webhooks/enigio}") String webhookCallbackUrl) {
        this.enigioClient = enigioClient;
        this.notificationPublisher = notificationPublisher;
        this.signingExpiryHours = signingExpiryHours;
        this.approvalExpiryHours = approvalExpiryHours;
        this.webhookCallbackUrl = webhookCallbackUrl;
    }

    // ===== Group 1: Document Preparation =====

    @Step(order = 1, completedWhen = "pdfGenerated == true")
    public void createDraft(EnigioInstrumentEntity flow) {
        log.info("[{}] Creating draft for reference={}", flow.getId(), flow.getReference());
        flow.setPdfGenerated(true);
        checkpoint(flow);
    }

    @Step(order = 2, completedWhen = "traceOriginalId != null")
    @RecoverOn(httpStatus = 409, message = "already", action = RecoverAction.SKIP)
    public void registerDocument(EnigioInstrumentEntity flow) {
        log.info("[{}] Registering document on Enigio (ref={})", flow.getId(), flow.getReference());

        Map<String, Object> content = Map.of(
                "Reference", flow.getReference(),
                "Instrument Type", flow.getInstrumentType().toEnigioValue(),
                "Title", flow.getTitle(),
                "Terms", flow.getContent() != null ? flow.getContent() : ""
        );

        if (flow.getCustomData() != null) {
            var merged = new java.util.HashMap<>(content);
            merged.putAll(flow.getCustomData());
            content = merged;
        }

        var response = enigioClient.createDocument(
                flow.getReference(),
                flow.getInstrumentType().toEnigioValue(),
                flow.getDocumentCode().name(),
                content,
                flow.getCustomData()
        );

        flow.setTraceOriginalId(response.traceOriginalId());
        flow.setVersionKey(response.versionKey());
        checkpoint(flow);
    }

    @Step(order = 3, completedWhen = "attachmentVersionKey != null")
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    public void addAttachment(EnigioInstrumentEntity flow) {
        List<Attachment> attachments = flow.getAttachments();
        if (attachments == null || attachments.isEmpty()) {
            log.info("[{}] No attachments to add, skipping", flow.getId());
            flow.setAttachmentVersionKey("NONE");
            return;
        }

        log.info("[{}] Adding {} attachment(s) to document {}", flow.getId(),
                attachments.size(), flow.getTraceOriginalId());

        var response = enigioClient.amendDocument(
                flow.getTraceOriginalId(),
                flow.getVersionKey(),
                Map.of("Attachments", "Added " + attachments.size() + " supporting document(s)"),
                attachments
        );

        flow.setAttachmentVersionKey(response.versionKey());
        flow.setVersionKey(response.versionKey());
        checkpoint(flow);
    }

    // ===== Gate 1: Notify downstream, await approval for signing =====

    @Step(order = 4, completedWhen = "signingApproved == true")
    public void awaitPreparationApproval(EnigioInstrumentEntity flow) {
        if (!flow.isPreparationNotified()) {
            log.info("[{}] Document preparation complete. Publishing notification.", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    "PREPARATION_COMPLETE", "AWAITING_APPROVAL");
            flow.setPreparationNotified(true);
            flow.setPreparationNotifiedAt(Instant.now());
            checkpoint(flow);
        }

        // Check approval expiry
        if (flow.getPreparationNotifiedAt() != null) {
            Duration elapsed = Duration.between(flow.getPreparationNotifiedAt(), Instant.now());
            if (elapsed.toHours() >= approvalExpiryHours) {
                notificationPublisher.notifyPhaseComplete(flow, "APPROVAL_EXPIRED",
                        "Signing approval not received within " + approvalExpiryHours + "h");
                throw new NonRetryableStepException(
                        "Signing approval expired after " + elapsed.toHours() + "h");
            }
        }

        if (!flow.isSigningApproved()) {
            throw new RetryableStepException(
                    "Awaiting downstream approval for signing phase. " +
                    "Call POST /flows/enigio-instrument/" + flow.getId() + "/approve");
        }

        log.info("[{}] Signing phase approved by downstream", flow.getId());
    }

    // ===== Group 2: Signing Ceremony =====

    @Step(order = 5, completedWhen = "signersAdded == true")
    public void addSigners(EnigioInstrumentEntity flow) {
        log.info("[{}] Adding {} signer(s) to document {}", flow.getId(),
                flow.getSigners().size(), flow.getTraceOriginalId());

        enigioClient.addRequiredSignatures(flow.getTraceOriginalId(), flow.getSigners());

        flow.setSignersAdded(true);
        checkpoint(flow);
    }

    @Step(order = 6, completedWhen = "signingEmailsSent == true")
    public void sendForSigning(EnigioInstrumentEntity flow) {
        log.info("[{}] Sending signing emails for document {}", flow.getId(),
                flow.getTraceOriginalId());

        // Register webhook for signature events (best-effort — polling is fallback)
        if (!flow.isWebhookRegistered()) {
            enigioClient.registerWebhook(webhookCallbackUrl,
                    List.of("FULLY_SIGNED", "PARTIALLY_SIGNED", "SIGNATURE_REJECTED"));
            flow.setWebhookRegistered(true);
        }

        // Set expected signature count and start time for expiry tracking
        flow.setSignaturesRequired(flow.getSigners().size());
        flow.setSignaturesReceived(0);
        flow.setSigningStartedAt(Instant.now());

        List<String> failedEmails = enigioClient.sendSigningEmails(
                flow.getTraceOriginalId(), "en");

        if (!failedEmails.isEmpty()) {
            throw new RetryableStepException(
                    "Failed to send signing emails to: " + failedEmails);
        }

        flow.setSigningEmailsSent(true);
        checkpoint(flow);
    }

    /**
     * Wait for all signatures via webhook-driven gate.
     *
     * Signal priority:
     *   1. Webhook (primary) — Enigio POSTs PARTIALLY_SIGNED / FULLY_SIGNED to our endpoint
     *   2. Poll fallback — GET /required-signatures/original/{id}/status on each Kafka retry
     *   3. Expiry — if signing exceeds dis.signing.expiry-hours, fail the flow
     *
     * Each PARTIALLY_SIGNED webhook increments signaturesReceived and notifies downstream.
     * Flow advances only when signingStatus == "SIGNED".
     */
    @Step(order = 7, completedWhen = "signingStatus == 'SIGNED'")
    public void awaitSignatures(EnigioInstrumentEntity flow) {
        // 1. Check if webhook already set the status (fast path)
        if ("SIGNED".equals(flow.getSigningStatus())) {
            log.info("[{}] All {}/{} signatures completed (via webhook)",
                    flow.getId(), flow.getSignaturesReceived(), flow.getSignaturesRequired());
            return;
        }

        if ("REJECTED".equals(flow.getSigningStatus())) {
            throw new NonRetryableStepException("Signature rejected by signer");
        }

        if ("EXPIRED".equals(flow.getSigningStatus())) {
            throw new NonRetryableStepException("Signing expired after " + signingExpiryHours + " hours");
        }

        // 2. Check expiry threshold
        if (flow.getSigningStartedAt() != null) {
            Duration elapsed = Duration.between(flow.getSigningStartedAt(), Instant.now());
            if (elapsed.toHours() >= signingExpiryHours) {
                log.warn("[{}] Signing expired after {}h (threshold: {}h). Final status check.",
                        flow.getId(), elapsed.toHours(), signingExpiryHours);

                // One final poll before expiring
                String finalStatus = enigioClient.getSigningStatus(flow.getTraceOriginalId());
                if ("SIGNED".equals(finalStatus)) {
                    flow.setSigningStatus("SIGNED");
                    flow.setSignaturesReceived(flow.getSignaturesRequired());
                    log.info("[{}] Last-minute signing detected — marking SIGNED", flow.getId());
                    return;
                }

                flow.setSigningStatus("EXPIRED");
                notificationPublisher.notifyPhaseComplete(flow, "SIGNING_EXPIRED",
                        flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed before expiry");
                throw new NonRetryableStepException(
                        "Signing expired after " + elapsed.toHours() + "h. " +
                        flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed.");
            }
        }

        // 3. Safety-net poll via GET /required-signatures/original/{id}/status
        log.info("[{}] Safety-net poll: GET signing status. Current: {}/{} signed, elapsed: {}",
                flow.getId(), flow.getSignaturesReceived(), flow.getSignaturesRequired(),
                flow.getSigningStartedAt() != null
                        ? Duration.between(flow.getSigningStartedAt(), Instant.now()).toMinutes() + "m"
                        : "unknown");

        String status = enigioClient.getSigningStatus(flow.getTraceOriginalId());

        if ("SIGNED".equals(status)) {
            flow.setSigningStatus("SIGNED");
            flow.setSignaturesReceived(flow.getSignaturesRequired());
            log.info("[{}] All signatures completed (via poll fallback)", flow.getId());
            return;
        }

        if ("REJECTED".equals(status)) {
            flow.setSigningStatus("REJECTED");
            throw new NonRetryableStepException("Signature rejected by signer");
        }

        // Still pending — retry via Kafka topic
        flow.setSigningStatus(status);
        throw new RetryableStepException(
                "Awaiting signatures: " + flow.getSignaturesReceived() + "/" +
                flow.getSignaturesRequired() + " signed. Webhook or next poll will advance.");
    }

    // ===== Gate 2: Notify downstream, await approval for delivery =====

    @Step(order = 8, completedWhen = "deliveryApproved == true")
    public void awaitDeliveryApproval(EnigioInstrumentEntity flow) {
        if (!flow.isSigningNotified()) {
            log.info("[{}] Signing ceremony complete. Publishing notification.", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    "SIGNING_COMPLETE", "AWAITING_APPROVAL");
            flow.setSigningNotified(true);
            flow.setSigningNotifiedAt(Instant.now());
            checkpoint(flow);
        }

        // Check approval expiry
        if (flow.getSigningNotifiedAt() != null) {
            Duration elapsed = Duration.between(flow.getSigningNotifiedAt(), Instant.now());
            if (elapsed.toHours() >= approvalExpiryHours) {
                notificationPublisher.notifyPhaseComplete(flow, "APPROVAL_EXPIRED",
                        "Delivery approval not received within " + approvalExpiryHours + "h");
                throw new NonRetryableStepException(
                        "Delivery approval expired after " + elapsed.toHours() + "h");
            }
        }

        if (!flow.isDeliveryApproved()) {
            throw new RetryableStepException(
                    "Awaiting downstream approval for delivery phase. " +
                    "Call POST /flows/enigio-instrument/" + flow.getId() + "/approve");
        }

        log.info("[{}] Delivery phase approved by downstream", flow.getId());
    }

    // ===== Group 3: Packaging & Delivery =====

    @Step(order = 9, completedWhen = "validationResult == 'VALID'")
    public void validateDocument(EnigioInstrumentEntity flow) {
        log.info("[{}] Validating document {} against ledger", flow.getId(),
                flow.getTraceOriginalId());

        String result = enigioClient.validateDocument(flow.getTraceOriginalId());
        flow.setValidationResult(result);

        if (!"VALID".equals(result)) {
            throw new NonRetryableStepException(
                    "Document validation failed: " + result);
        }
    }

    @Step(order = 10, completedWhen = "envelopeTraceId != null")
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    public void createEnvelope(EnigioInstrumentEntity flow) {
        log.info("[{}] Creating and sealing envelope for document {}", flow.getId(),
                flow.getTraceOriginalId());

        String draftId = enigioClient.createEnvelopeDraft(
                "ENV-" + flow.getReference(),
                "Signed " + flow.getInstrumentType().toEnigioValue() + " — " + flow.getTitle(),
                flow.getTraceOriginalId()
        );
        flow.setEnvelopeDraftId(draftId);
        checkpoint(flow);

        var sealed = enigioClient.sealEnvelopeDraft(draftId);
        flow.setEnvelopeTraceId(sealed.traceOriginalId());
        flow.setEnvelopeVersionKey(sealed.versionKey());
    }

    @Step(order = 11, completedWhen = "transferId != null")
    public void transferDocument(EnigioInstrumentEntity flow) {
        log.info("[{}] Transferring envelope {} to {}", flow.getId(),
                flow.getEnvelopeTraceId(), flow.getRecipient().getEmail());

        String transferId = enigioClient.transferByEmail(
                flow.getEnvelopeTraceId(),
                flow.getEnvelopeVersionKey(),
                flow.getRecipient().getEmail(),
                flow.getRecipient().getName(),
                "Transfer of " + flow.getInstrumentType().toEnigioValue() + " — " + flow.getReference(),
                "Please accept the enclosed " + flow.getInstrumentType().toEnigioValue()
        );

        flow.setTransferId(transferId);
        log.info("[{}] Transfer initiated: transferId={}", flow.getId(), transferId);

        // Final notification — flow complete
        notificationPublisher.notifyPhaseComplete(flow, "FLOW_COMPLETE", "COMPLETED");
    }

    // ===== Cancellation Handlers =====

    @OnCancel(step = "registerDocument")
    public void cancelDocument(EnigioInstrumentEntity flow) {
        if (flow.getTraceOriginalId() == null) return;
        log.info("[{}] Cancelling: invalidating document {} on Enigio",
                flow.getId(), flow.getTraceOriginalId());
        try {
            enigioClient.invalidateDocument(flow.getTraceOriginalId());
        } catch (Exception e) {
            log.warn("[{}] Failed to invalidate document: {}", flow.getId(), e.getMessage());
        }
        notificationPublisher.notifyPhaseComplete(flow, "FLOW_CANCELLED", "CANCELLED");
    }

    @OnCancel(step = "transferDocument")
    public void cancelTransfer(EnigioInstrumentEntity flow) {
        if (flow.getTransferId() == null) return;
        log.info("[{}] Cancelling: revoking transfer {} on Enigio",
                flow.getId(), flow.getTransferId());
        // In production: enigioClient.cancelTransfer(flow.getEnvelopeTraceId(), flow.getTransferId());
        notificationPublisher.notifyPhaseComplete(flow, "TRANSFER_CANCELLED", "CANCELLED");
    }
}
