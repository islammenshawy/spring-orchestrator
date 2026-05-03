package com.enigio.orchestrator.mock.service;

import com.enigio.orchestrator.mock.config.FailureConfig;
import com.enigio.orchestrator.mock.dto.*;
import com.enigio.orchestrator.mock.model.FailureScenario;
import com.enigio.orchestrator.mock.model.MockDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class MockDocumentService {

    private final FailureConfig failureConfig;
    private final Map<String, MockDocument> documents = new ConcurrentHashMap<>();
    private final Map<String, MockDocument> envelopeDrafts = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> registeredWebhooks = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicLong signatureIdCounter = new AtomicLong(1);

    // ===== Documents =====

    public DocumentOperationResponse createDocument(CreateDocumentRequest req) {
        applyFailure("createDocument");
        simulateDelay(200, 500);

        String traceOriginalId = generateTraceId();
        String versionKey = generateVersionKey();

        MockDocument doc = MockDocument.builder()
                .traceOriginalId(traceOriginalId)
                .versionKey(versionKey)
                .version(1)
                .reference(req.getReference())
                .documentType(req.getDocumentType())
                .documentCode(req.getDocumentCode())
                .format(req.getFormat() != null ? req.getFormat() : "PDF")
                .content(req.getContent())
                .customData(req.getCustomData())
                .build();

        documents.put(traceOriginalId, doc);
        log.info("Created document: {} (ref={}, type={}, code={})",
                traceOriginalId, req.getReference(), req.getDocumentType(), req.getDocumentCode());

        return new DocumentOperationResponse(traceOriginalId, versionKey);
    }

    public DocumentOperationResponse amendDocument(String traceOriginalId, AmendDocumentRequest req) {
        applyFailure("amendDocument");
        simulateDelay(200, 400);

        MockDocument doc = getDocument(traceOriginalId);

        if (!doc.getVersionKey().equals(req.getVersionKey())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Version key mismatch. Expected: " + doc.getVersionKey());
        }

        // Merge amendment content
        if (doc.getContent() != null) {
            doc.getContent().putAll(req.getContent());
        } else {
            doc.setContent(req.getContent());
        }

        doc.setVersion(doc.getVersion() + 1);
        String newVersionKey = generateVersionKey();
        doc.setVersionKey(newVersionKey);

        log.info("Amended document: {} (version={})", traceOriginalId, doc.getVersion());
        return new DocumentOperationResponse(traceOriginalId, newVersionKey);
    }

    public ValidateDocumentResponse validateDocument(ValidateDocumentRequest req) {
        applyFailure("validateDocument");
        simulateDelay(100, 300);

        // Mock: treat document field as traceOriginalId for lookup
        String traceId = req.getDocument();
        MockDocument doc = documents.get(traceId);

        if (doc == null) {
            return new ValidateDocumentResponse("CANNOT_VALIDATE",
                    "Document not found in ledger", null);
        }

        if (doc.isInvalidated()) {
            return new ValidateDocumentResponse("NOT_VALID",
                    "Document has been invalidated", traceId);
        }

        doc.setValidated(true);
        log.info("Validated document: {} -> VALID", traceId);
        return new ValidateDocumentResponse("VALID", null, traceId);
    }

    // ===== Required Signatures =====

    public void addRequiredSignatures(String traceOriginalId, List<AddRequiredSignature> signers) {
        applyFailure("addRequiredSignatures");
        simulateDelay(150, 350);

        MockDocument doc = getDocument(traceOriginalId);

        List<RequiredSignatureDTO> signatures = new ArrayList<>();
        for (AddRequiredSignature req : signers) {
            List<RequiredSignatureDTO.SignerDTO> signerDTOs = new ArrayList<>();
            if (req.getSigners() != null) {
                for (AddRequiredSignature.AddSigner s : req.getSigners()) {
                    signerDTOs.add(RequiredSignatureDTO.SignerDTO.builder()
                            .email(s.getEmail())
                            .name(s.getName())
                            .phone(s.getPhone())
                            .linkId(UUID.randomUUID().toString())
                            .signingLink("https://mock.traceoriginal.com/sign/" + UUID.randomUUID())
                            .build());
                }
            }

            signatures.add(RequiredSignatureDTO.builder()
                    .id(signatureIdCounter.getAndIncrement())
                    .traceOriginalId(traceOriginalId)
                    .capacityOfSignatory(req.getCapacityOfSignatory())
                    .documentVersion(doc.getVersion())
                    .organisation(req.getOrganisation())
                    .role(req.getRole())
                    .status("PENDING")
                    .linkCreatedAt(Instant.now().toString())
                    .linkExpiresAt(req.getLinkExpiresAt())
                    .signers(signerDTOs)
                    .build());
        }

        doc.getRequiredSignatures().addAll(signatures);
        log.info("Added {} required signatures to document {}", signers.size(), traceOriginalId);
    }

    public List<String> sendSigningEmails(SendSignEmailRequest req) {
        applyFailure("sendSigningEmails");
        simulateDelay(200, 400);

        MockDocument doc = getDocument(req.getTraceOriginalId());
        doc.setSigningEmailsSent(true);

        log.info("Signing emails sent for document {} (locale={})",
                req.getTraceOriginalId(), req.getLocale());

        // Auto-fire webhook callbacks after a delay (simulates signers completing)
        if (!registeredWebhooks.isEmpty()) {
            simulateSigningWebhooks(req.getTraceOriginalId());
        }

        return List.of();
    }

    public void registerWebhook(String url, Map<String, Object> config) {
        registeredWebhooks.add(Map.of("url", url, "config", config));
        log.info("Webhook registered: {} (total: {})", url, registeredWebhooks.size());
    }

    public String getSigningStatus(String traceOriginalId) {
        applyFailure("getSigningStatus");
        simulateDelay(100, 200);

        MockDocument doc = getDocument(traceOriginalId);
        doc.setSigningPollCount(doc.getSigningPollCount() + 1);

        // Simulate: PENDING on first poll, SIGNED on second
        String status;
        if (doc.getRequiredSignatures().isEmpty()) {
            status = "NO_SIGNATURE";
        } else if (doc.getSigningPollCount() > 1) {
            status = "SIGNED";
            doc.getRequiredSignatures().forEach(s -> s.setStatus("SIGNED"));
        } else {
            status = "PENDING";
        }

        log.info("Signing status for {}: {} (poll #{})",
                traceOriginalId, status, doc.getSigningPollCount());
        return status;
    }

    /**
     * Simulate signing completion — fires webhook callbacks for each signer.
     * Called by the mock admin to trigger realistic webhook flow.
     */
    public void simulateSigningWebhooks(String traceOriginalId) {
        MockDocument doc = getDocument(traceOriginalId);
        int signerCount = doc.getRequiredSignatures().size();

        Thread.startVirtualThread(() -> {
            try {
                var webClient = org.springframework.web.reactive.function.client.WebClient.create();

                for (int i = 0; i < signerCount; i++) {
                    Thread.sleep(2000); // 2s between each signer

                    boolean isLast = (i == signerCount - 1);
                    String eventType = isLast ? "FULLY_SIGNED" : "PARTIALLY_SIGNED";

                    for (Map<String, Object> webhook : registeredWebhooks) {
                        String url = (String) webhook.get("url");
                        try {
                            webClient.post().uri(url)
                                    .bodyValue(Map.of(
                                            "messageId", UUID.randomUUID().toString(),
                                            "traceOriginalId", traceOriginalId,
                                            "eventType", eventType,
                                            "documentCode", doc.getDocumentCode() != null ? doc.getDocumentCode() : "NEG",
                                            "reference", doc.getReference() != null ? doc.getReference() : "",
                                            "documentType", doc.getDocumentType() != null ? doc.getDocumentType() : ""
                                    ))
                                    .retrieve().bodyToMono(String.class).block();
                            log.info("Webhook fired: {} → {} for {}", eventType, url, traceOriginalId);
                        } catch (Exception e) {
                            log.warn("Webhook call failed to {}: {}", url, e.getMessage());
                        }
                    }

                    doc.getRequiredSignatures().get(i).setStatus("SIGNED");
                }
                doc.getRequiredSignatures().forEach(s -> s.setStatus("SIGNED"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public List<RequiredSignatureDTO> getRequiredSignatures(String traceOriginalId) {
        MockDocument doc = getDocument(traceOriginalId);
        return doc.getRequiredSignatures();
    }

    // ===== Envelopes =====

    public EnvelopeDraftResponse createEnvelopeDraft(EnvelopeDraftRequest req) {
        applyFailure("createEnvelopeDraft");
        simulateDelay(200, 400);

        String draftId = "draft_" + UUID.randomUUID().toString().replace("-", "");

        // Validate that referenced documents exist
        if (req.getOriginalDocuments() != null) {
            for (EnvelopeDraftRequest.OriginalDocumentRef ref : req.getOriginalDocuments()) {
                getDocument(ref.getTraceOriginalId());
            }
        }

        MockDocument envelope = MockDocument.builder()
                .envelopeDraftId(draftId)
                .reference(req.getReference())
                .content(Map.of("coverMessage", req.getCoverMessage()))
                .build();

        envelopeDrafts.put(draftId, envelope);
        log.info("Created envelope draft: {} (ref={})", draftId, req.getReference());

        return new EnvelopeDraftResponse(draftId);
    }

    public Map<String, Object> uploadAdditionalDocument(String draftId, String filename,
                                                          String fileHash, byte[] data) {
        applyFailure("uploadAdditionalDocument");
        simulateDelay(100, 300);

        MockDocument draft = envelopeDrafts.get(draftId);
        if (draft == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Envelope draft not found: " + draftId);
        }

        String fileId = UUID.randomUUID().toString();
        draft.getAdditionalDocuments().add(Map.of(
                "fileId", fileId,
                "filename", filename,
                "fileHash", fileHash,
                "fileSize", data.length
        ));

        log.info("Added additional document '{}' to draft {} → fileId={}", filename, draftId, fileId);
        return Map.of("fileId", fileId);
    }

    public List<Map<String, Object>> getAdditionalDocumentMetadata(String draftId) {
        MockDocument draft = envelopeDrafts.get(draftId);
        if (draft == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Envelope draft not found: " + draftId);
        }
        return draft.getAdditionalDocuments();
    }

    public DocumentOperationResponse sealEnvelopeDraft(String draftId) {
        applyFailure("sealEnvelopeDraft");
        simulateDelay(300, 600);

        MockDocument draft = envelopeDrafts.get(draftId);
        if (draft == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Envelope draft not found: " + draftId);
        }

        String envelopeTraceId = generateTraceId();
        String versionKey = generateVersionKey();

        draft.setEnvelopeTraceId(envelopeTraceId);
        draft.setEnvelopeVersionKey(versionKey);

        // Store as a document too
        documents.put(envelopeTraceId, MockDocument.builder()
                .traceOriginalId(envelopeTraceId)
                .versionKey(versionKey)
                .version(1)
                .reference(draft.getReference())
                .documentType("Envelope")
                .documentCode("ENV")
                .content(draft.getContent())
                .build());

        log.info("Sealed envelope draft {} -> traceOriginalId={}", draftId, envelopeTraceId);
        return new DocumentOperationResponse(envelopeTraceId, versionKey);
    }

    // ===== Transfer =====

    public TransferEnvelopeByEmailResponse transferByEmail(
            String traceOriginalId, TransferEnvelopeByEmailRequest req) {
        applyFailure("transferByEmail");
        simulateDelay(200, 500);

        MockDocument doc = getDocument(traceOriginalId);

        if (!doc.getVersionKey().equals(req.getVersionKey())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Version key mismatch for transfer");
        }

        String transferId = UUID.randomUUID().toString();
        doc.setTransferId(transferId);
        doc.setInTransit(true);

        log.info("Transfer initiated for {} -> {} (transferId={})",
                traceOriginalId, req.getRecipientEmail(), transferId);

        // Simulate recipient accepting after 3s delay (fires TRANSFER webhook)
        simulateTransferAcceptanceWebhook(traceOriginalId);

        return new TransferEnvelopeByEmailResponse(transferId);
    }

    private void simulateTransferAcceptanceWebhook(String envelopeTraceId) {
        Thread.startVirtualThread(() -> {
            try {
                Thread.sleep(3000); // Simulate recipient reviewing + accepting after 3s
                log.info("Simulating TRANSFER acceptance for envelope {} ({} webhook(s) registered)",
                        envelopeTraceId, registeredWebhooks.size());
                var webClient = org.springframework.web.reactive.function.client.WebClient.create();
                for (Map<String, Object> webhook : registeredWebhooks) {
                    String url = (String) webhook.get("url");
                    if (url == null) continue;
                    try {
                        webClient.post().uri(url)
                                .bodyValue(Map.of(
                                        "messageId", UUID.randomUUID().toString(),
                                        "traceOriginalId", envelopeTraceId,
                                        "eventType", "TRANSFER",
                                        "timestamp", java.time.Instant.now().toString()
                                ))
                                .retrieve().bodyToMono(String.class).block();
                        log.info("Webhook fired: TRANSFER → {} for {}", url, envelopeTraceId);
                    } catch (Exception e) {
                        log.warn("Transfer webhook failed to {}: {}", url, e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("Transfer webhook simulation failed for {}: {}", envelopeTraceId, e.getMessage());
            }
        });
    }

    // ===== Supplementary Document Operations =====

    public Map<String, Object> getDocumentData(String traceOriginalId) {
        MockDocument doc = getDocument(traceOriginalId);
        return Map.of(
                "contentType", "application/" + doc.getFormat().toLowerCase(),
                "data", "mock-base64-content",
                "filename", doc.getReference() + "." + doc.getFormat().toLowerCase()
        );
    }

    public Map<String, Object> getDocumentMetadata(String traceOriginalId) {
        MockDocument doc = getDocument(traceOriginalId);
        return Map.of(
                "traceOriginalId", doc.getTraceOriginalId(),
                "reference", doc.getReference() != null ? doc.getReference() : "",
                "documentType", doc.getDocumentType() != null ? doc.getDocumentType() : "",
                "documentCode", doc.getDocumentCode() != null ? doc.getDocumentCode() : "",
                "format", doc.getFormat() != null ? doc.getFormat() : "PDF",
                "version", doc.getVersion(),
                "invalidated", doc.isInvalidated(),
                "inTransit", doc.isInTransit(),
                "ownerKey", "mock-owner-key-" + traceOriginalId.substring(0, 8),
                "versionCreatedAt", doc.getCreatedAt().toString()
        );
    }

    public void invalidateDocument(String traceOriginalId) {
        applyFailure("invalidateDocument");
        MockDocument doc = getDocument(traceOriginalId);
        if (doc.isInvalidated()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Document already invalidated");
        }
        doc.setInvalidated(true);
        log.info("Invalidated document: {}", traceOriginalId);
    }

    public Map<String, Object> getTechnicalDetails(String traceOriginalId) {
        MockDocument doc = getDocument(traceOriginalId);
        return Map.of(
                "traceOriginalId", doc.getTraceOriginalId(),
                "versionKey", doc.getVersionKey(),
                "version", doc.getVersion(),
                "createdAt", doc.getCreatedAt().toString()
        );
    }

    // ===== Admin =====

    public void resetAll() {
        documents.clear();
        envelopeDrafts.clear();
        signatureIdCounter.set(1);
        failureConfig.reset();
        log.info("All mock state reset");
    }

    // ===== Internal =====

    private MockDocument getDocument(String traceOriginalId) {
        MockDocument doc = documents.get(traceOriginalId);
        if (doc == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Document not found: " + traceOriginalId);
        }
        return doc;
    }

    private String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "") +
               UUID.randomUUID().toString().replace("-", "").substring(0, 32);
    }

    private String generateVersionKey() {
        return UUID.randomUUID().toString().replace("-", "") +
               UUID.randomUUID().toString().replace("-", "").substring(0, 32);
    }

    private void applyFailure(String endpoint) {
        FailureScenario scenario = failureConfig.getFailureFor(endpoint);
        switch (scenario) {
            case TIMEOUT -> {
                try { Thread.sleep(120_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            case HTTP_400 -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Simulated 400 for " + endpoint);
            case HTTP_403 -> throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Simulated 403 for " + endpoint);
            case HTTP_409 -> throw new ResponseStatusException(HttpStatus.CONFLICT, "Resource already exists for " + endpoint);
            case HTTP_429 -> throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Simulated 429 for " + endpoint);
            case HTTP_500 -> throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Simulated 500 for " + endpoint);
            case HTTP_502 -> throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Simulated 502 for " + endpoint);
            case HTTP_503 -> throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Simulated 503 for " + endpoint);
            case FLAKY -> {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Simulated flaky error for " + endpoint);
                }
            }
            case NONE -> { }
        }
    }

    private void simulateDelay(int minMs, int maxMs) {
        try { Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs)); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
