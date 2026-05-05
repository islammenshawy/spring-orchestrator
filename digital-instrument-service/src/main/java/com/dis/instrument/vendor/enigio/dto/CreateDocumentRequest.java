package com.dis.instrument.vendor.enigio.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

/**
 * Enigio POST /api/v1/documents — create a new trace:original document.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateDocumentRequest(

        @NotBlank(message = "reference is required by Enigio API")
        String reference,

        @NotBlank(message = "documentType is required (e.g. 'Promissory note')")
        String documentType,

        @NotBlank(message = "documentCode is required (e.g. 'NEG', 'NON_NEG')")
        String documentCode,

        @NotNull(message = "content map is required")
        Map<String, Object> content,

        Map<String, Object> customData
) {}
