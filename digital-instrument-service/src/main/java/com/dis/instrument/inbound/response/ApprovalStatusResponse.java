package com.dis.instrument.inbound.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Current approval status of a flow")
public record ApprovalStatusResponse(
        String instrumentId,
        @Schema(description = "Current phase label", example = "signing")
        String phase,
        @Schema(description = "Flow status", example = "IN_PROGRESS")
        String status,
        boolean preparationNotified,
        boolean signingApproved,
        boolean signingNotified,
        boolean deliveryApproved
) {}
