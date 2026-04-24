package com.enigio.orchestrator.mock.controller;

import com.enigio.orchestrator.mock.model.MockDocument;
import com.enigio.orchestrator.mock.service.MockDocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class MockDocumentController {

    private final MockDocumentService service;

    @PostMapping("/documents")
    public ResponseEntity<Map<String, String>> createDocument(@RequestBody Map<String, String> request) {
        MockDocument doc = service.createDocument(
                request.get("title"),
                request.get("content"),
                request.get("metadata"));
        return ResponseEntity.ok(Map.of(
                "documentId", doc.getDocumentId(),
                "status", "CREATED"));
    }

    @PostMapping("/documents/{id}/attachments")
    public ResponseEntity<Map<String, String>> uploadAttachment(
            @PathVariable("id") String documentId,
            @RequestBody Map<String, String> request) {
        String attachmentId = service.uploadAttachment(
                documentId,
                request.get("fileName"),
                request.get("fileContent"));
        return ResponseEntity.ok(Map.of(
                "attachmentId", attachmentId,
                "status", "UPLOADED"));
    }

    @PostMapping("/documents/{id}/sign")
    public ResponseEntity<Map<String, String>> requestSignature(
            @PathVariable("id") String documentId,
            @RequestBody Map<String, String> request) {
        String signatureRequestId = service.requestSignature(documentId, request.get("signerEmail"));
        return ResponseEntity.ok(Map.of(
                "signatureRequestId", signatureRequestId,
                "status", "SIGNATURE_REQUESTED"));
    }

    @GetMapping("/documents/{id}/signature-status")
    public ResponseEntity<Map<String, Object>> verifySignature(
            @PathVariable("id") String documentId,
            @RequestParam("signatureRequestId") String signatureRequestId) {
        boolean verified = service.verifySignature(documentId, signatureRequestId);
        return ResponseEntity.ok(Map.of(
                "signatureRequestId", signatureRequestId,
                "status", verified ? "VERIFIED" : "PENDING",
                "verified", verified));
    }

    @PostMapping("/documents/{id}/finalize")
    public ResponseEntity<Map<String, String>> finalizeDocument(@PathVariable("id") String documentId) {
        MockDocument doc = service.finalizeDocument(documentId);
        return ResponseEntity.ok(Map.of(
                "finalDocumentUrl", "https://enigio.com/docs/" + doc.getDocumentId(),
                "traceHash", UUID.randomUUID().toString(),
                "status", "FINALIZED"));
    }
}
