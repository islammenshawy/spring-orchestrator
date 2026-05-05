package com.dis.instrument.vendor.enigio.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DocumentOperationResponse(
        String traceOriginalId,
        String versionKey
) {}
