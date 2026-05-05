package com.dis.instrument.vendor.enigio.feign.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

/**
 * Enigio POST /api/v1/envelopes/{id}/transfer-by-email — initiate transfer to recipient.
 * Recipient receives an email with a link to accept the envelope.
 */
public record TransferRequest(

        @NotBlank(message = "recipientEmail is required")
        @Email(message = "recipientEmail must be a valid email address")
        String recipientEmail,

        @NotBlank(message = "recipientName is required")
        String recipientName,

        String transferComment,
        String transferEmailMessage,

        @NotBlank(message = "versionKey is required for transfer")
        String versionKey
) {}
