package com.enigio.orchestrator.mock.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Matches Enigio trace:original API v3.3 — ErrorResponse
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    private String timestamp;
    private String message;
    private int code;
    private String details;

    public ErrorResponse(String message, int code) {
        this.timestamp = Instant.now().toString();
        this.message = message;
        this.code = code;
    }
}
