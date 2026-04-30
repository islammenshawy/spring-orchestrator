package com.enigio.orchestrator.mock.controller;

import com.enigio.orchestrator.mock.dto.*;
import com.enigio.orchestrator.mock.service.MockDocumentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Mock implementation of Enigio trace:original Fullnode API v3.3.
 * Paths match the real API exactly: https://docs.traceoriginal.com
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class MockDocumentController {

    private final MockDocumentService service;

    // ===== Documents =====

    /** POST /api/v1/documents — Create a new trace:original document */
    @PostMapping("/documents")
    public ResponseEntity<DocumentOperationResponse> createDocument(
            @Valid @RequestBody CreateDocumentRequest request) {
        return ResponseEntity.ok(service.createDocument(request));
    }

    /** POST /api/v1/documents/{traceOriginalId}/amend — Add amendment to document */
    @PostMapping("/documents/{traceOriginalId}/amend")
    public ResponseEntity<DocumentOperationResponse> amendDocument(
            @PathVariable String traceOriginalId,
            @Valid @RequestBody AmendDocumentRequest request) {
        return ResponseEntity.ok(service.amendDocument(traceOriginalId, request));
    }

    /** POST /api/v1/documents/validate — Validate document against ledger */
    @PostMapping("/documents/validate")
    public ResponseEntity<ValidateDocumentResponse> validateDocument(
            @Valid @RequestBody ValidateDocumentRequest request) {
        return ResponseEntity.ok(service.validateDocument(request));
    }

    /** GET /api/v1/documents/{traceOriginalId} — Download document */
    @GetMapping("/documents/{traceOriginalId}")
    public ResponseEntity<Map<String, Object>> downloadDocument(@PathVariable String traceOriginalId) {
        return ResponseEntity.ok(service.getDocumentData(traceOriginalId));
    }

    /** GET /api/v1/documents/{traceOriginalId}/metadata — Get document metadata */
    @GetMapping("/documents/{traceOriginalId}/metadata")
    public ResponseEntity<Map<String, Object>> getDocumentMetadata(@PathVariable String traceOriginalId) {
        return ResponseEntity.ok(service.getDocumentMetadata(traceOriginalId));
    }

    /** POST /api/v1/documents/{traceOriginalId}/invalidate — Invalidate document */
    @PostMapping("/documents/{traceOriginalId}/invalidate")
    public ResponseEntity<Void> invalidateDocument(@PathVariable String traceOriginalId) {
        service.invalidateDocument(traceOriginalId);
        return ResponseEntity.ok().build();
    }

    /** GET /api/v1/documents/{traceOriginalId}/technical-details/latest — Get latest version key */
    @GetMapping("/documents/{traceOriginalId}/technical-details/latest")
    public ResponseEntity<Map<String, Object>> getLatestTechnicalDetails(@PathVariable String traceOriginalId) {
        return ResponseEntity.ok(service.getTechnicalDetails(traceOriginalId));
    }

    // ===== Required Signatures =====

    /** POST /api/v1/required-signatures/original/{traceOriginalId} — Add required signatures */
    @PostMapping("/required-signatures/original/{traceOriginalId}")
    public ResponseEntity<Void> addRequiredSignatures(
            @PathVariable String traceOriginalId,
            @Valid @RequestBody List<AddRequiredSignature> signers) {
        service.addRequiredSignatures(traceOriginalId, signers);
        return ResponseEntity.ok().build();
    }

    /** GET /api/v1/required-signatures/original/{traceOriginalId} — Get required signatures */
    @GetMapping("/required-signatures/original/{traceOriginalId}")
    public ResponseEntity<List<RequiredSignatureDTO>> getRequiredSignatures(
            @PathVariable String traceOriginalId) {
        return ResponseEntity.ok(service.getRequiredSignatures(traceOriginalId));
    }

    /** GET /api/v1/required-signatures/original/{traceOriginalId}/status — Get signing status */
    @GetMapping("/required-signatures/original/{traceOriginalId}/status")
    public ResponseEntity<String> getSigningStatus(@PathVariable String traceOriginalId) {
        String status = service.getSigningStatus(traceOriginalId);
        // Real API returns plain string enum
        return ResponseEntity.ok("\"" + status + "\"");
    }

    /** POST /api/v1/required-signatures/send-sign-emails — Send signing emails */
    @PostMapping("/required-signatures/send-sign-emails")
    public ResponseEntity<List<String>> sendSigningEmails(
            @Valid @RequestBody SendSignEmailRequest request) {
        return ResponseEntity.ok(service.sendSigningEmails(request));
    }

    // ===== Webhooks =====

    /** POST /api/v1/notifications/webhooks — Register webhook */
    @PostMapping("/notifications/webhooks")
    public ResponseEntity<Map<String, Object>> registerWebhook(@RequestBody Map<String, Object> request) {
        String url = (String) request.get("url");
        service.registerWebhook(url, request);
        return ResponseEntity.ok(Map.of(
                "id", java.util.UUID.randomUUID().toString(),
                "url", url != null ? url : "",
                "events", request.getOrDefault("events", List.of()),
                "signatureMethod", request.getOrDefault("signatureMethod", "NONE")
        ));
    }

    // ===== Envelopes =====

    /** POST /api/v1/envelopes/drafts — Create envelope draft */
    @PostMapping("/envelopes/drafts")
    public ResponseEntity<EnvelopeDraftResponse> createEnvelopeDraft(
            @Valid @RequestBody EnvelopeDraftRequest request) {
        return ResponseEntity.ok(service.createEnvelopeDraft(request));
    }

    /** POST /api/v1/envelopes/drafts/{draftId}/seal — Seal envelope draft */
    @PostMapping("/envelopes/drafts/{draftId}/seal")
    public ResponseEntity<DocumentOperationResponse> sealEnvelopeDraft(
            @PathVariable String draftId) {
        return ResponseEntity.ok(service.sealEnvelopeDraft(draftId));
    }

    /** POST /api/v1/envelopes/{traceOriginalId}/transfer-by-email — Transfer envelope */
    @PostMapping("/envelopes/{traceOriginalId}/transfer-by-email")
    public ResponseEntity<TransferEnvelopeByEmailResponse> transferByEmail(
            @PathVariable String traceOriginalId,
            @Valid @RequestBody TransferEnvelopeByEmailRequest request) {
        return ResponseEntity.ok(service.transferByEmail(traceOriginalId, request));
    }
}
