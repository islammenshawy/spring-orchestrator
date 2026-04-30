package com.enigio.orchestrator.mock.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Matches Enigio trace:original API v3.3 — ValidateDocumentResponse
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidateDocumentResponse {
    private String result;          // VALID | NOT_VALID | OUTDATED | CANNOT_VALIDATE
    private String details;         // error description if not valid
    private String traceOriginalId;
}
