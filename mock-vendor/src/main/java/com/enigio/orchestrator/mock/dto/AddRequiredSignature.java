package com.enigio.orchestrator.mock.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/required-signatures/original/{traceOriginalId}
 * Request body is an array of AddRequiredSignature.
 */
@Data
public class AddRequiredSignature {

    @NotBlank(message = "capacityOfSignatory is required")
    private String capacityOfSignatory; // e.g., "CEO", "Authorized Signatory"

    private Integer documentVersion;
    private String linkExpiresAt; // ISO-8601 datetime
    private String organisation;
    private String role;
    private Boolean truncationOfExpiryDateOff;

    @NotEmpty(message = "signers is required")
    @Valid
    private List<AddSigner> signers;

    @Data
    public static class AddSigner {

        @NotBlank(message = "email is required")
        private String email;

        @NotBlank(message = "phone is required")
        private String phone;

        private String name;
        private List<AddSignerIdentification> identifications;
    }

    @Data
    public static class AddSignerIdentification {

        @NotBlank(message = "eid is required")
        private String eid;

        @NotBlank(message = "eidMethod is required")
        private String eidMethod; // DOCUSIGN, ADOBE_SIGN, BANKID, ENIGIO_SIGN

        @NotBlank(message = "eidType is required")
        private String eidType; // EMAIL, SMS, etc.
    }
}
