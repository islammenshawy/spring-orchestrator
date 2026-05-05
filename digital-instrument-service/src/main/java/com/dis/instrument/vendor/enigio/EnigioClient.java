package com.dis.instrument.vendor.enigio;

import com.dis.instrument.model.Attachment;
import com.dis.instrument.model.Signer;
import com.dis.instrument.vendor.enigio.dto.VendorDocumentMetadata;
import com.dis.instrument.vendor.enigio.dto.VendorDocumentResponse;
import com.dis.instrument.vendor.enigio.dto.VendorRequiredSignature;
import com.dis.instrument.vendor.enigio.dto.VendorTechnicalDetails;
import com.dis.instrument.vendor.enigio.feign.EnigioDocumentClient;
import com.dis.instrument.vendor.enigio.feign.EnigioEnvelopeClient;
import com.dis.instrument.vendor.enigio.feign.EnigioSignatureClient;
import com.dis.instrument.vendor.enigio.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Facade over Feign clients for Enigio trace:original API v3.3.
 * Translates domain objects to/from vendor API shapes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EnigioClient {

    private final EnigioDocumentClient documentClient;
    private final EnigioSignatureClient signatureClient;
    private final EnigioEnvelopeClient envelopeClient;

    // ===== Documents =====

    public record DocumentResponse(String traceOriginalId, String versionKey) {}

    public DocumentResponse createDocument(String reference, String documentType,
                                           String documentCode, Map<String, Object> content,
                                           Map<String, Object> customData) {
        var res = documentClient.createDocument(new CreateDocumentRequest(
                reference, documentType, documentCode, content, customData));
        return new DocumentResponse(res.traceOriginalId(), res.versionKey());
    }

    public DocumentResponse amendDocument(String traceOriginalId, String versionKey,
                                          Map<String, Object> content,
                                          List<Attachment> attachments) {
        List<AmendDocumentRequest.AttachmentPayload> payloads = null;
        if (attachments != null && !attachments.isEmpty()) {
            payloads = attachments.stream()
                    .map(a -> new AmendDocumentRequest.AttachmentPayload(
                            a.getFilename(), a.getData(),
                            a.getComment() != null ? a.getComment() : ""))
                    .toList();
        }

        var res = documentClient.amendDocument(traceOriginalId,
                new AmendDocumentRequest(versionKey, content, payloads));
        return new DocumentResponse(res.traceOriginalId(), res.versionKey());
    }

    public void invalidateDocument(String traceOriginalId, String versionKey, String comment) {
        documentClient.invalidateDocument(traceOriginalId,
                new InvalidateRequest(
                        versionKey != null ? versionKey : "",
                        comment != null ? comment : "Document voided"));
        log.info("Invalidated document: {}", traceOriginalId);
    }

    public String validateDocument(String traceOriginalId) {
        return documentClient.validateDocument(new ValidateRequest(traceOriginalId)).result();
    }

    public VendorDocumentResponse getDocument(String traceOriginalId) {
        return documentClient.getDocument(traceOriginalId);
    }

    public VendorDocumentMetadata getDocumentMetadata(String traceOriginalId) {
        return documentClient.getDocumentMetadata(traceOriginalId);
    }

    public VendorTechnicalDetails getTechnicalDetails(String traceOriginalId) {
        return documentClient.getTechnicalDetails(traceOriginalId);
    }

    // ===== Signatures =====

    public void addRequiredSignatures(String traceOriginalId, List<Signer> signers) {
        var body = signers.stream()
                .map(s -> {
                    var signer = new HashMap<String, Object>();
                    signer.put("capacityOfSignatory", s.getCapacity());
                    signer.put("organisation", s.getOrganisation());
                    signer.put("signers", List.of(Map.of(
                            "email", s.getEmail(),
                            "phone", s.getPhone(),
                            "name", s.getName())));
                    return (Map<String, Object>) signer;
                })
                .collect(Collectors.toList());

        signatureClient.addRequiredSignatures(traceOriginalId, body);
    }

    public List<String> sendSigningEmails(String traceOriginalId, String locale) {
        return signatureClient.sendSigningEmails(
                new SendSigningEmailsRequest(traceOriginalId, locale));
    }

    public String getSigningStatus(String traceOriginalId) {
        String status = signatureClient.getSigningStatus(traceOriginalId);
        return status != null ? status.replace("\"", "") : "PENDING";
    }

    public void registerWebhook(String callbackUrl, List<String> events) {
        try {
            signatureClient.registerWebhook(
                    new WebhookRegistrationRequest(callbackUrl, events, "NONE"));
            log.info("Webhook registered: {} for events {}", callbackUrl, events);
        } catch (Exception e) {
            log.warn("Failed to register webhook (polling fallback active): {}", e.getMessage());
        }
    }

    public List<VendorRequiredSignature> getRequiredSignatures(String traceOriginalId) {
        return signatureClient.getRequiredSignatures(traceOriginalId);
    }

    // ===== Envelopes =====

    public String createEnvelopeDraft(String reference, String coverMessage,
                                      String originalDocumentTraceId) {
        return envelopeClient.createEnvelopeDraft(new EnvelopeDraftRequest(
                reference, coverMessage,
                List.of(new EnvelopeDraftRequest.OriginalDocumentRef(originalDocumentTraceId))
        )).draftId();
    }

    public DocumentResponse sealEnvelopeDraft(String draftId) {
        var res = envelopeClient.sealEnvelopeDraft(draftId);
        return new DocumentResponse(res.traceOriginalId(), res.versionKey());
    }

    public String uploadAdditionalDocument(String draftId, String filename,
                                           String sha256, byte[] data) {
        var res = envelopeClient.uploadAdditionalDocument(draftId, filename, sha256, data);
        log.info("Uploaded additional document '{}' to draft {} -> fileId={}", filename, draftId, res.fileId());
        return res.fileId();
    }

    public String transferByEmail(String envelopeTraceId, String versionKey,
                                  String recipientEmail, String recipientName,
                                  String comment, String emailMessage) {
        return envelopeClient.transferByEmail(envelopeTraceId, new TransferRequest(
                recipientEmail, recipientName, comment, emailMessage, versionKey
        )).transferId();
    }

    public void cancelEnvelopeTransfer(String transferId) {
        envelopeClient.cancelEnvelopeTransfer(transferId);
        log.info("Cancelled envelope transfer: {}", transferId);
    }
}
