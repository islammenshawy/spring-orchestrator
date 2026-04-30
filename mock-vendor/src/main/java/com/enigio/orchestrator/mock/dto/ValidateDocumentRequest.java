package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/documents/validate
 * Real API accepts binary document. Mock accepts traceOriginalId for simplicity.
 */
@Data
public class ValidateDocumentRequest {
    @NotBlank(message = "document is required")
    private String document; // In real API: base64 binary. Mock: traceOriginalId lookup.
}
