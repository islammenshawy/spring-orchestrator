package com.dis.instrument.inbound.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Error response")
public record ErrorResponse(
        String error,
        String instrumentId,
        String phase,
        String message
) {
    public ErrorResponse(String error) {
        this(error, null, null, null);
    }

    public ErrorResponse(String error, String instrumentId) {
        this(error, instrumentId, null, null);
    }
}
