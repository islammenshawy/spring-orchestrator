package com.dis.instrument.vendor.enigio.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Enigio POST /api/v1/documents/{id}/amend — add content/attachments to a document.
 * Requires the current versionKey (from create or previous amend).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AmendDocumentRequest(

        @NotBlank(message = "versionKey is required — use value from create/previous amend response")
        String versionKey,

        @NotNull(message = "content map is required")
        Map<String, Object> content,

        List<AttachmentPayload> attachments
) {
    public record AttachmentPayload(
            @NotBlank String filename,
            @NotBlank String data,
            String comment
    ) {}
}
