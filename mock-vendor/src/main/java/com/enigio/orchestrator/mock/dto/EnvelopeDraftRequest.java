package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/envelopes/drafts
 */
@Data
public class EnvelopeDraftRequest {

    @NotBlank(message = "coverMessage is required")
    private String coverMessage;

    @NotBlank(message = "reference is required")
    @Size(min = 1, max = 250)
    private String reference;

    @Size(max = 25)
    private List<OriginalDocumentRef> originalDocuments;

    @Size(max = 25)
    private List<CopyDocumentRef> copies;

    private String ownerKey;
    private String redirectUrl;

    @Data
    public static class OriginalDocumentRef {
        @NotBlank
        private String traceOriginalId;
        private Long version;
    }

    @Data
    public static class CopyDocumentRef {
        @NotBlank
        private String traceOriginalId;
        private Long version;
    }
}
