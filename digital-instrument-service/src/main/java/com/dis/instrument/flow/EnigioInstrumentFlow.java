package com.dis.instrument.flow;

import com.dis.instrument.vendor.enigio.EnigioClient;
import com.dis.instrument.service.NotificationService;
import com.dis.instrument.model.AdditionalDocument;
import com.dis.instrument.model.SigningStatus;
import com.dis.instrument.model.FlowPhase;
import com.dis.instrument.model.WebhookEvent;
import com.dis.instrument.model.AdditionalDocumentRepository;
import com.dis.instrument.model.Attachment;
import com.dis.instrument.model.PriorityUpdate;
import com.dis.instrument.model.Signer;
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
    private final AdditionalDocumentRepository additionalDocumentRepository;
    private final NotificationService notificationPublisher;
    private final long signingExpiryHours;
    private final long approvalExpiryHours;
    private final String webhookCallbackUrl;

    public EnigioInstrumentFlow(EnigioClient enigioClient,
                                AdditionalDocumentRepository additionalDocumentRepository,
                                NotificationService notificationPublisher,
                                @Value("${dis.signing.expiry-hours:48}") long signingExpiryHours,
                                @Value("${dis.approval.expiry-hours:72}") long approvalExpiryHours,
                                @Value("${dis.webhook.callback-url:http://digital-instrument-service:8087/webhooks/enigio}") String webhookCallbackUrl) {
        this.enigioClient = enigioClient;
        this.additionalDocumentRepository = additionalDocumentRepository;
        this.notificationPublisher = notificationPublisher;
        this.signingExpiryHours = signingExpiryHours;
        this.approvalExpiryHours = approvalExpiryHours;
        this.webhookCallbackUrl = webhookCallbackUrl;
    }

    // ===== Group 1: Document Preparation =====

    @Step(order = 1)
    public void createDraft(EnigioInstrumentEntity flow) {
        log.info("[{}] Creating draft for reference={}", flow.getId(), flow.getReference());
        flow.setPdfGenerated(true);
        checkpoint(flow);
    }

    @Step(order = 2)
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

    @Step(order = 3)
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

    @Step(order = 4)
    public void awaitPreparationApproval(EnigioInstrumentEntity flow) {
        if (!flow.isPreparationNotified()) {
            log.info("[{}] Document preparation complete. Publishing notification.", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    FlowPhase.PREPARATION_COMPLETE.name(), "AWAITING_APPROVAL");
            flow.setPreparationNotified(true);
            flow.setPreparationNotifiedAt(Instant.now());
            checkpoint(flow);
        }

        // Check approval expiry
        if (flow.getPreparationNotifiedAt() != null) {
            Duration elapsed = Duration.between(flow.getPreparationNotifiedAt(), Instant.now());
            if (elapsed.toHours() >= approvalExpiryHours) {
                notifyBestEffort(flow, FlowPhase.APPROVAL_EXPIRED.name(),
                        "Signing approval not received within " + approvalExpiryHours + "h");
                throw new NonRetryableStepException(
                        "Signing approval expired after " + elapsed.toHours() + "h");
            }
        }

        waitUntil(() -> flow.isSigningApproved(), Duration.ofHours(approvalExpiryHours));

        log.info("[{}] Signing phase approved by downstream", flow.getId());
    }

    // ===== Group 2: Signing Ceremony =====

    @Step(order = 5)
    public void addSigners(EnigioInstrumentEntity flow) {
        log.info("[{}] Adding {} signer(s) to document {}", flow.getId(),
                flow.getSigners().size(), flow.getTraceOriginalId());

        enigioClient.addRequiredSignatures(flow.getTraceOriginalId(), flow.getSigners());

        flow.setSignersAdded(true);
        checkpoint(flow);
    }

    @Step(order = 6)
    public void sendForSigning(EnigioInstrumentEntity flow) {
        log.info("[{}] Sending signing emails for document {}", flow.getId(),
                flow.getTraceOriginalId());

        // Register webhook for signature events (best-effort — polling is fallback)
        if (!flow.isWebhookRegistered()) {
            enigioClient.registerWebhook(webhookCallbackUrl,
                    List.of(WebhookEvent.FULLY_SIGNED.name(), WebhookEvent.PARTIALLY_SIGNED.name(),
                            WebhookEvent.SIGNATURE_REJECTED.name(), WebhookEvent.TRANSFER.name(),
                            WebhookEvent.TRANSFER_REJECTED.name(), WebhookEvent.CREATE.name(),
                            WebhookEvent.AMENDMENT.name(), WebhookEvent.INVALIDATE.name()));
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
     * Flow advances only when signingStatus == SigningStatus.SIGNED.name().
     */
    @Step(order = 7)
    public void awaitSignatures(EnigioInstrumentEntity flow) {
        // 1. Check if webhook already set the status (fast path)
        if (SigningStatus.SIGNED.name().equals(flow.getSigningStatus())) {
            log.info("[{}] All {}/{} signatures completed (via webhook)",
                    flow.getId(), flow.getSignaturesReceived(), flow.getSignaturesRequired());
            return;
        }

        if (SigningStatus.REJECTED.name().equals(flow.getSigningStatus())) {
            throw new NonRetryableStepException("Signature rejected by signer");
        }

        if (SigningStatus.EXPIRED.name().equals(flow.getSigningStatus())) {
            throw new NonRetryableStepException("Signing expired after " + signingExpiryHours + " hours");
        }

        // 2. Check expiry threshold
        if (flow.getSigningStartedAt() != null) {
            Duration elapsed = Duration.between(flow.getSigningStartedAt(), Instant.now());
            if (elapsed.toHours() >= signingExpiryHours) {
                log.error("[{}] Signing expired after {}h (threshold: {}h). traceOriginalId={}, signers={}/{}",
                        flow.getId(), elapsed.toHours(), signingExpiryHours,
                        flow.getTraceOriginalId(), flow.getSignaturesReceived(), flow.getSignaturesRequired());

                // One final poll before expiring
                String finalStatus = enigioClient.getSigningStatus(flow.getTraceOriginalId());
                if (SigningStatus.SIGNED.name().equals(finalStatus)) {
                    flow.setSigningStatus(SigningStatus.SIGNED.name());
                    flow.setSignaturesReceived(flow.getSignaturesRequired());
                    log.info("[{}] Last-minute signing detected — marking SIGNED", flow.getId());
                    return;
                }

                flow.setSigningStatus(SigningStatus.EXPIRED.name());
                notifyBestEffort(flow, FlowPhase.SIGNING_EXPIRED.name(),
                        flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed before expiry");
                throw new NonRetryableStepException(
                        "Signing expired after " + elapsed.toHours() + "h. " +
                        flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed.");
            }
        }

        // 3. Safety-net poll via GET /required-signatures/original/{id}/status
        log.debug("[{}] Safety-net poll: GET signing status. Current: {}/{} signed, elapsed: {}",
                flow.getId(), flow.getSignaturesReceived(), flow.getSignaturesRequired(),
                flow.getSigningStartedAt() != null
                        ? Duration.between(flow.getSigningStartedAt(), Instant.now()).toMinutes() + "m"
                        : "unknown");

        String status = enigioClient.getSigningStatus(flow.getTraceOriginalId());

        if (SigningStatus.SIGNED.name().equals(status)) {
            flow.setSigningStatus(SigningStatus.SIGNED.name());
            flow.setSignaturesReceived(flow.getSignaturesRequired());
            log.info("[{}] All signatures completed (via poll fallback)", flow.getId());
            return;
        }

        if (SigningStatus.REJECTED.name().equals(status)) {
            flow.setSigningStatus(SigningStatus.REJECTED.name());
            throw new NonRetryableStepException("Signature rejected by signer");
        }

        // Still pending — park and wait for webhook or next poll
        flow.setSigningStatus(status);
        waitUntil(() -> SigningStatus.SIGNED.name().equals(flow.getSigningStatus()), Duration.ofHours(signingExpiryHours));
    }

    // ===== Gate 2: Notify downstream, await approval for delivery =====

    @Step(order = 8)
    public void awaitDeliveryApproval(EnigioInstrumentEntity flow) {
        if (!flow.isSigningNotified()) {
            log.info("[{}] Signing ceremony complete. Publishing notification.", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    FlowPhase.SIGNING_COMPLETE.name(), "AWAITING_APPROVAL");
            flow.setSigningNotified(true);
            flow.setSigningNotifiedAt(Instant.now());
            checkpoint(flow);
        }

        // Check approval expiry
        if (flow.getSigningNotifiedAt() != null) {
            Duration elapsed = Duration.between(flow.getSigningNotifiedAt(), Instant.now());
            if (elapsed.toHours() >= approvalExpiryHours) {
                notifyBestEffort(flow, FlowPhase.APPROVAL_EXPIRED.name(),
                        "Delivery approval not received within " + approvalExpiryHours + "h");
                throw new NonRetryableStepException(
                        "Delivery approval expired after " + elapsed.toHours() + "h");
            }
        }

        waitUntil(() -> flow.isDeliveryApproved(), Duration.ofHours(approvalExpiryHours));

        log.info("[{}] Delivery phase approved by downstream", flow.getId());
    }

    // ===== Group 3: Packaging & Delivery =====

    @Step(order = 9)
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

    @Step(order = 10)
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    public void createEnvelope(EnigioInstrumentEntity flow) {
        log.info("[{}] Creating and sealing envelope for document {}", flow.getId(),
                flow.getTraceOriginalId());

        // 1. Create draft (if not already created on a previous retry)
        if (flow.getEnvelopeDraftId() == null) {
            String draftId = enigioClient.createEnvelopeDraft(
                    "ENV-" + flow.getReference(),
                    "Signed " + flow.getInstrumentType().toEnigioValue() + " — " + flow.getTitle(),
                    flow.getTraceOriginalId()
            );
            flow.setEnvelopeDraftId(draftId);
            checkpoint(flow);
        }

        // 2. Upload additional documents from MongoDB (if any, and not already uploaded)
        if (!flow.isAdditionalDocsUploaded()
                && flow.getAdditionalDocumentIds() != null
                && !flow.getAdditionalDocumentIds().isEmpty()) {

            List<AdditionalDocument> docs = additionalDocumentRepository
                    .findAllById(flow.getAdditionalDocumentIds());

            if (docs.size() != flow.getAdditionalDocumentIds().size()) {
                log.error("[{}] Expected {} additional documents but found {} in MongoDB — some may have been deleted",
                        flow.getId(), flow.getAdditionalDocumentIds().size(), docs.size());
            }

            log.info("[{}] Uploading {} additional document(s) to envelope draft {}",
                    flow.getId(), docs.size(), flow.getEnvelopeDraftId());

            for (AdditionalDocument doc : docs) {
                byte[] decoded = java.util.Base64.getDecoder().decode(doc.getData());
                enigioClient.uploadAdditionalDocument(
                        flow.getEnvelopeDraftId(),
                        doc.getFilename(),
                        doc.getSha256Hash(),
                        decoded
                );
            }

            flow.setAdditionalDocsUploaded(true);
            checkpoint(flow);
        }

        // 3. Seal the draft
        var sealed = enigioClient.sealEnvelopeDraft(flow.getEnvelopeDraftId());
        flow.setEnvelopeTraceId(sealed.traceOriginalId());
        flow.setEnvelopeVersionKey(sealed.versionKey());
        checkpoint(flow);
    }

    @Step(order = 11)
    public void transferDocument(EnigioInstrumentEntity flow) {
        // Phase 1: Initiate transfer (if not already done)
        if (flow.getTransferId() == null) {
            log.info("[{}] Transferring envelope {} to recipient", flow.getId(),
                    flow.getEnvelopeTraceId());

            String transferId = enigioClient.transferByEmail(
                    flow.getEnvelopeTraceId(),
                    flow.getEnvelopeVersionKey(),
                    flow.getRecipient().getEmail(),
                    flow.getRecipient().getName(),
                    "Transfer of " + flow.getInstrumentType().toEnigioValue() + " — " + flow.getReference(),
                    "Please accept the enclosed " + flow.getInstrumentType().toEnigioValue()
            );

            flow.setTransferId(transferId);
            flow.setTransferInitiatedAt(Instant.now());
            checkpoint(flow);

            log.info("[{}] Transfer initiated: transferId={}", flow.getId(), transferId);
            notifyBestEffort(flow, FlowPhase.TRANSFER_INITIATED.name(), "AWAITING_RECIPIENT");
        }

        // Phase 2: Check transfer outcome (set by TRANSFER / TRANSFER_REJECTED webhook)
        if (flow.isTransferAccepted()) {
            log.info("[{}] Transfer accepted by recipient", flow.getId());
            notifyBestEffort(flow, FlowPhase.FLOW_COMPLETE.name(), "COMPLETED");
            return;
        }

        if (flow.isTransferRejected()) {
            log.error("[{}] Transfer REJECTED by recipient. transferId={}", flow.getId(), flow.getTransferId());
            notifyBestEffort(flow, FlowPhase.TRANSFER_REJECTED.name(), "FAILED");
            throw new NonRetryableStepException("Transfer rejected by recipient");
        }

        // Phase 3: Check transfer expiry
        if (flow.getTransferInitiatedAt() != null) {
            Duration elapsed = Duration.between(flow.getTransferInitiatedAt(), Instant.now());
            if (elapsed.toHours() >= approvalExpiryHours) {
                log.error("[{}] Transfer expired after {}h. transferId={}",
                        flow.getId(), elapsed.toHours(), flow.getTransferId());
                notifyBestEffort(flow, FlowPhase.TRANSFER_EXPIRED.name(),
                        "Recipient did not accept within " + approvalExpiryHours + "h");
                throw new NonRetryableStepException(
                        "Transfer expired after " + elapsed.toHours() + "h — recipient did not accept");
            }
        }

        // Still waiting — park in DB, TRANSFER/TRANSFER_REJECTED webhook will re-activate
        waitUntil(() -> flow.isTransferAccepted(), Duration.ofHours(approvalExpiryHours));
    }

    // ===== Cancellation Handlers =====
    //
    // Runs in reverse order when POST /cancel is called.
    // Each handler must be idempotent and handle vendor errors gracefully.
    //
    // Enigio invalidation rules:
    //   - Requires versionKey (must be current version)
    //   - 400 if already invalidated → safe to skip
    //   - 400 if inTransit → must cancel transfer first
    //   - 404 if document doesn't exist → safe to skip

    /**
     * Invalidate the document on Enigio.
     * This is the primary cancel action — voids the document permanently.
     * All pending signatures become invalid automatically.
     */
    @OnCancel(step = "registerDocument")
    public void cancelDocument(EnigioInstrumentEntity flow) {
        if (flow.getTraceOriginalId() == null) {
            log.info("[{}] No traceOriginalId — nothing to invalidate", flow.getId());
            return;
        }

        log.info("[{}] Cancelling: invalidating document {} (versionKey={})",
                flow.getId(), flow.getTraceOriginalId(), flow.getVersionKey());

        try {
            enigioClient.invalidateDocument(flow.getTraceOriginalId(), flow.getVersionKey(),
                    "Flow cancelled: " + (flow.getErrorMessage() != null ? flow.getErrorMessage() : "user requested"));
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("404") || msg.contains("not found")) {
                log.info("[{}] Document not found on Enigio (already removed) — skipping", flow.getId());
            } else if (msg.contains("already invalidated") || msg.contains("end state")) {
                log.info("[{}] Document already invalidated — skipping", flow.getId());
            } else if (msg.contains("inTransit") || msg.contains("in transit")) {
                log.warn("[{}] Document is inTransit — must cancel transfer first", flow.getId());
                // Try to cancel transfer before invalidating
                cancelEnvelopeTransfer(flow);
                // Retry invalidation
                try {
                    enigioClient.invalidateDocument(flow.getTraceOriginalId(), flow.getVersionKey(),
                            "Flow cancelled after transfer cancellation");
                } catch (Exception retryEx) {
                    log.error("[{}] Failed to invalidate after transfer cancel: {}", flow.getId(), retryEx.getMessage());
                }
            } else {
                log.error("[{}] Failed to invalidate document: {} traceOriginalId={}", flow.getId(), msg, flow.getTraceOriginalId());
            }
        }
    }

    /**
     * Invalidate the sealed envelope on Enigio (if created).
     * Only needed if the envelope was sealed but not yet transferred.
     */
    @OnCancel(step = "createEnvelope")
    public void cancelEnvelope(EnigioInstrumentEntity flow) {
        if (flow.getEnvelopeTraceId() == null) return;

        log.info("[{}] Cancelling: invalidating envelope {}", flow.getId(), flow.getEnvelopeTraceId());
        try {
            enigioClient.invalidateDocument(flow.getEnvelopeTraceId(), flow.getEnvelopeVersionKey(),
                    "Envelope cancelled: flow voided");
        } catch (Exception e) {
            log.error("[{}] Failed to invalidate envelope: {} envelopeTraceId={}", flow.getId(), e.getMessage(), flow.getEnvelopeTraceId());
        }
    }

    /**
     * Cancel a pending envelope transfer on Enigio.
     * Only works if the recipient has NOT yet opened the envelope.
     * If recipient already opened → transfer cannot be cancelled (too late).
     */
    @OnCancel(step = "transferDocument")
    public void cancelEnvelopeTransfer(EnigioInstrumentEntity flow) {
        if (flow.getTransferId() == null) return;

        log.info("[{}] Cancelling: revoking envelope transfer {} (envelope={})",
                flow.getId(), flow.getTransferId(), flow.getEnvelopeTraceId());

        try {
            enigioClient.cancelEnvelopeTransfer(flow.getTransferId());
            log.info("[{}] Transfer cancelled successfully", flow.getId());
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("already accepted") || msg.contains("already opened")) {
                log.error("[{}] CANNOT CANCEL — recipient already opened the envelope. " +
                        "Legal possession transferred. Invalidation requires separate process.",
                        flow.getId());
            } else if (msg.contains("404") || msg.contains("not found")) {
                log.info("[{}] Transfer not found (already completed/cancelled) — skipping", flow.getId());
            } else {
                log.error("[{}] Failed to cancel transfer: {} transferId={}", flow.getId(), msg, flow.getTransferId());
            }
        }

        notifyBestEffort(flow, FlowPhase.FLOW_CANCELLED.name(), "CANCELLED");
    }

    /** Best-effort notification — logs failure but doesn't throw. Used for non-critical
     *  notifications (expiry, completion, cancellation) where the primary action should
     *  proceed regardless of notification delivery. */
    private void notifyBestEffort(EnigioInstrumentEntity flow, String phase, String status) {
        try {
            notificationPublisher.notifyPhaseComplete(flow, phase, status);
        } catch (Exception e) {
            log.warn("[{}] Best-effort notification failed (phase={}, status={}): {}",
                    flow.getId(), phase, status, e.getMessage());
        }
    }

    // ========== Signals ==========

    @Signal
    public void updatePriority(EnigioInstrumentEntity flow, PriorityUpdate data) {
        log.info("[{}] Priority updated to {} via signal (reason: {})",
                flow.getId(), data.getPriority(), data.getReason());
        flow.setPriority(data.getPriority().name());
    }

    @Signal
    public void requestCancellation(EnigioInstrumentEntity flow) {
        if (SigningStatus.SIGNED.name().equals(flow.getSigningStatus())) {
            throw new IllegalStateException("Cannot cancel — document already signed");
        }
        log.info("[{}] Cancellation requested via signal", flow.getId());
    }
}
