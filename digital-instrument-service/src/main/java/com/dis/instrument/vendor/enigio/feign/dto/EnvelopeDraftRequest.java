package com.dis.instrument.vendor.enigio.feign.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

/**
 * Enigio POST /api/v1/envelopes/drafts — create a new envelope draft.
 * At least one original document must be included.
 */
public record EnvelopeDraftRequest(

        @NotBlank(message = "reference is required")
        String reference,

        String coverMessage,

        @NotEmpty(message = "at least one original document is required")
        List<OriginalDocumentRef> originalDocuments
) {
    public record OriginalDocumentRef(
            @NotBlank String traceOriginalId
    ) {}
}
