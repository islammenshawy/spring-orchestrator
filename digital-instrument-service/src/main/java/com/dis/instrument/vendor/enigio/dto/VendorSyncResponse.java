package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Aggregated vendor state for a single document — returned by the sync endpoint.
 */
@Schema(description = "Aggregated vendor state for a trace:original document. "
        + "Only requested sections are populated; others are null.")
public record VendorSyncResponse(

        @Schema(description = "The trace:original ID queried",
                example = "60fcd0a7d84b1b5df0542b29b7a941abe6b45b20de7585ff2e91cbaf1665dac5")
        String traceOriginalId,

        @Schema(description = "Document content (base-64 payload). Included when `include=document`.",
                nullable = true)
        VendorDocumentResponse document,

        @Schema(description = "Document metadata (type, status flags). Included when `include=metadata`.",
                nullable = true)
        VendorDocumentMetadata metadata,

        @Schema(description = "Latest technical details (versionKey, ledger hash). Included when `include=technicalDetails`.",
                nullable = true)
        VendorTechnicalDetails technicalDetails,

        @Schema(description = "Required signatures and their status. Included when `include=requiredSignatures`.",
                nullable = true)
        List<VendorRequiredSignature> requiredSignatures
) {}
