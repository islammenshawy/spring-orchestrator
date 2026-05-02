package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Maps to Enigio ErrorResponse (v3.3).
 */
@Schema(description = "Error response from Enigio trace:original")
public record VendorErrorResponse(

        @Schema(description = "UTC timestamp when the error occurred", example = "2024-03-09T10:08:56.246Z")
        String timestamp,

        @Schema(description = "Human-readable error description", example = "Document was not found")
        String message,

        @Schema(description = "Enigio internal error code", example = "3500")
        Integer code,

        @Schema(description = "HTTP status", example = "NOT_FOUND")
        String status,

        @Schema(description = "Field-level validation details (optional)")
        List<ErrorDetail> details
) {
    public record ErrorDetail(
            @Schema(description = "Field that caused the error", example = "versionKey")
            String fieldName,

            @Schema(description = "Validation error message", example = "must not be null")
            String errorMessage
    ) {}
}
