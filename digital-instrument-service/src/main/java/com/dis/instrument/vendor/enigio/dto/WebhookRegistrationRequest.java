package com.dis.instrument.vendor.enigio.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

/**
 * Enigio POST /api/v1/notifications/webhooks — register a webhook callback.
 * Events: FULLY_SIGNED, PARTIALLY_SIGNED, SIGNATURE_REJECTED, TRANSFER,
 *         TRANSFER_REJECTED, CREATE, AMENDMENT, INVALIDATE.
 */
public record WebhookRegistrationRequest(
        @NotBlank(message = "callback URL is required")
        String url,

        @NotEmpty(message = "at least one event type is required")
        List<String> events,

        String signatureMethod
) {}
