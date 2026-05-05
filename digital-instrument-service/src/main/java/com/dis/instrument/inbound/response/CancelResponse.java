package com.dis.instrument.inbound.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Response after cancelling a flow")
public record CancelResponse(
        String instrumentId,
        @Schema(description = "Flow status after cancellation", example = "CANCELLED")
        String status,
        String message
) {}
