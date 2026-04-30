package com.enigio.orchestrator.mock.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Matches Enigio trace:original API v3.3 — EnvelopeDraftResponse
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnvelopeDraftResponse {
    private String draftId;
}
