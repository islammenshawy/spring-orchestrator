package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/documents/{traceOriginalId}/amend
 */
@Data
public class AmendDocumentRequest {

    @NotNull(message = "content is required")
    private Map<String, Object> content;

    @NotBlank(message = "versionKey is required")
    private String versionKey;

    private List<CreateDocumentRequest.AttachmentRequest> attachments;
}
