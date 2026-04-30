package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/envelopes/{traceOriginalId}/transfer-by-email
 */
@Data
public class TransferEnvelopeByEmailRequest {

    @NotBlank(message = "recipientEmail is required")
    private String recipientEmail;

    @NotBlank(message = "recipientName is required")
    private String recipientName;

    @NotBlank(message = "transferComment is required")
    private String transferComment;

    @NotBlank(message = "transferEmailMessage is required")
    private String transferEmailMessage;

    @NotBlank(message = "versionKey is required")
    private String versionKey;
}
