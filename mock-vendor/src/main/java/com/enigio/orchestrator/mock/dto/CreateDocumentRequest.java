package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/documents
 */
@Data
public class CreateDocumentRequest {

    @NotNull(message = "content is required")
    private Map<String, Object> content;

    @NotBlank(message = "documentCode is required")
    private String documentCode; // NEG | TTL | RGS | RGN | AGT

    @NotBlank(message = "documentType is required")
    @Size(min = 1, max = 250)
    private String documentType;

    @NotBlank(message = "reference is required")
    @Size(min = 1, max = 250)
    private String reference;

    // Optional fields
    private String format;          // PDF (default) | YAML
    private String locale;          // en | sv
    private InitialPDF initialPDF;
    private Map<String, Object> customData;
    private List<String> customDataSchemaUrls;
    private String contentSchemaUrl;
    private String ownerKey;
    private Boolean renderCustomData;
    private List<AttachmentRequest> attachments;

    @Data
    public static class InitialPDF {
        private String data; // base64-encoded PDF
        private String pdfFormOperation; // FILL_PDF_FORM_WITH_CUSTOM_DATA | EXTRACT_PDF_FORM_TO_CUSTOM_DATA
    }

    @Data
    public static class AttachmentRequest {
        @NotBlank(message = "filename is required")
        private String filename;

        @NotBlank(message = "data is required")
        private String data; // base64 binary

        private String comment;
    }
}
