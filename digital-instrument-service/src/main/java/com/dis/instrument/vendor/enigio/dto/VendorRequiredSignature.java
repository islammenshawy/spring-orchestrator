package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Maps to Enigio RequiredSignatureDTO (v3.3).
 */
@Schema(description = "Required signature entry from Enigio trace:original")
public record VendorRequiredSignature(

        @Schema(description = "Internal ID of this required signature entry", example = "1")
        Long id,

        @Schema(description = "Capacity of the signatory", example = "Signatory")
        String capacityOfSignatory,

        @Schema(description = "Organisation the signer represents", example = "Acme Corp")
        String organisation,

        @Schema(description = "Role of the signer", example = "Signer")
        String role,

        @Schema(description = "Signing status", allowableValues = {"PENDING", "SIGNED", "REJECTED", "NO_SIGNATURE"},
                example = "PENDING")
        String status,

        @Schema(description = "Document version to be signed", example = "1")
        Integer documentVersion,

        @Schema(description = "ISO-8601 timestamp when the signing link was created", example = "2024-04-16T19:01:18.591")
        String linkCreatedAt,

        @Schema(description = "ISO-8601 timestamp when the signing link expires", example = "2024-05-16T19:01:18.591")
        String linkExpiresAt,

        @Schema(description = "List of possible signers for this signature field")
        List<SignerInfo> signers
) {
    @Schema(description = "Individual signer details")
    public record SignerInfo(
            @Schema(description = "Signer email address", example = "john@example.com")
            String email,

            @Schema(description = "Signer full name", example = "John Doe")
            String name,

            @Schema(description = "Signer phone number", example = "+46701234567")
            String phone
    ) {}
}
