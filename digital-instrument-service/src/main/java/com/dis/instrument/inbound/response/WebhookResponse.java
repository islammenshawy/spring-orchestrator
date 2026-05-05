package com.dis.instrument.inbound.response;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Webhook acknowledgement")
public record WebhookResponse(
        @Schema(example = "received")
        String status,
        String eventType
) {}
