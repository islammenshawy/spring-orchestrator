package com.enigio.orchestrator.mock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Matches Enigio trace:original API v3.3 — POST /api/v1/required-signatures/send-sign-emails
 */
@Data
public class SendSignEmailRequest {

    @NotBlank(message = "traceOriginalId is required")
    private String traceOriginalId;

    @NotBlank(message = "locale is required")
    private String locale; // en | sv

    private Boolean force; // force re-send even if previously sent
}
