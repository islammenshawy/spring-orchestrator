package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Maps to Enigio DownloadDocumentResponse (v3.3).
 */
@Schema(description = "Document content returned from Enigio trace:original")
public record VendorDocumentResponse(

        @Schema(description = "MIME content type of the document", example = "application/pdf",
                allowableValues = {"application/pdf", "text/yaml"})
        String contentType,

        @Schema(description = "Base-64 encoded file content")
        String data,

        @Schema(description = "Original filename of the document", example = "contract-123.pdf")
        String filename
) {}
