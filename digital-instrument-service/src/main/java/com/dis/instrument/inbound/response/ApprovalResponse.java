package com.dis.instrument.inbound.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Response after approving a flow phase")
public record ApprovalResponse(
        @Schema(description = "Instrument ID")
        String instrumentId,
        @Schema(description = "Approved phase", example = "signing")
        String approvedPhase,
        @Schema(description = "Current phase label")
        String phase,
        String message
) {}
