package com.dis.instrument.inbound.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Response after starting a new instrument flow")
public record FlowStartedResponse(
        @Schema(description = "Instrument ID", example = "682b3f1a0000000000000001")
        String id,
        @Schema(description = "Flow status", example = "IN_PROGRESS")
        String status,
        @Schema(description = "Correlation ID for tracking", example = "550e8400-e29b-41d4-a716-446655440000")
        String correlationId,
        @Schema(description = "Current step being executed", example = "CREATE_DRAFT")
        String currentStep,
        @Schema(description = "Flow type identifier", example = "enigio-instrument")
        String flowType,
        String message
) {}
