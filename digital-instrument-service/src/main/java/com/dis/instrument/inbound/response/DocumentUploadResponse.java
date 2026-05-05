package com.dis.instrument.inbound.response;

import com.dis.instrument.model.AdditionalDocument;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Response after uploading an additional document")
public record DocumentUploadResponse(
        String id,
        String filename,
        String sha256Hash,
        long sizeBytes,
        String uploadedAt
) {
    public static DocumentUploadResponse from(AdditionalDocument doc) {
        return new DocumentUploadResponse(
                doc.getId(),
                doc.getFilename(),
                doc.getSha256Hash(),
                doc.getSizeBytes(),
                doc.getUploadedAt().toString()
        );
    }
}
