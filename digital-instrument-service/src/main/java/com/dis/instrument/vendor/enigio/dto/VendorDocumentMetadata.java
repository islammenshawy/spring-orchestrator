package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Maps to Enigio DocumentMetadataResponse (v3.3).
 */
@Schema(description = "Document metadata from Enigio trace:original")
public record VendorDocumentMetadata(

        @Schema(description = "The trace:original ID of the document",
                example = "60fcd0a7d84b1b5df0542b29b7a941abe6b45b20de7585ff2e91cbaf1665dac5")
        String traceOriginalId,

        @Schema(description = "Customer-assigned reference", example = "CONTRACT-2024-001")
        String reference,

        @Schema(description = "The document type", example = "Promissory Note")
        String documentType,

        @Schema(description = "Document code: NEG (Negotiable), TTL (Title), RGS (Registry), RGN (Registry no update), AGT (Agreement)",
                allowableValues = {"NEG", "TTL", "RGS", "RGN", "AGT", "ENV"}, example = "NEG")
        String documentCode,

        @Schema(description = "Document format", allowableValues = {"PDF", "YAML"}, example = "PDF")
        String format,

        @Schema(description = "Current version number (increments on each amendment)", example = "3")
        Long version,

        @Schema(description = "ISO-8601 timestamp of the current version creation", example = "2024-04-03T13:27:00.901Z")
        String versionCreatedAt,

        @Schema(description = "True if the document is a copy (not owned by this node)")
        Boolean copy,

        @Schema(description = "True if the document is currently being transferred — no operations allowed until transfer completes or is cancelled")
        Boolean inTransit,

        @Schema(description = "True if the document has been invalidated (end state — no further operations possible)")
        Boolean invalidated,

        @Schema(description = "Public key of the current document owner")
        String ownerKey
) {}
