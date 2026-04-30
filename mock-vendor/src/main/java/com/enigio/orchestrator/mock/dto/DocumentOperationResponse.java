package com.enigio.orchestrator.mock.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Matches Enigio trace:original API v3.3 — DocumentOperationResponse
 * Returned by: POST /documents, POST /documents/{id}/amend, POST /envelopes/drafts/{id}/seal
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DocumentOperationResponse {
    private String traceOriginalId;
    private String versionKey;
}
