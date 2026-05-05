package com.dis.instrument.vendor.enigio.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Enigio POST /api/v1/required-signatures/send-sign-emails — send signing invitation emails.
 */
public record SendSigningEmailsRequest(
        @NotBlank(message = "traceOriginalId is required")
        String traceOriginalId,

        @NotBlank(message = "locale is required (e.g. 'en', 'sv')")
        String locale
) {}
