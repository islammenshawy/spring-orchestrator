package com.dis.instrument.vendor.enigio.feign.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Enigio POST /api/v1/documents/{id}/invalidate — void a document permanently.
 * Requires current versionKey. Document enters end state after invalidation.
 */
public record InvalidateRequest(
        @NotBlank(message = "versionKey is required for invalidation")
        String versionKey,
        String comment
) {}
