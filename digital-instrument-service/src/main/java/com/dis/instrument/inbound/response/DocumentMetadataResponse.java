package com.dis.instrument.inbound.response;

import com.dis.instrument.model.AdditionalDocument;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Additional document metadata")
public record DocumentMetadataResponse(
        String id,
        String filename,
        String contentType,
        String sha256Hash,
        long sizeBytes,
        String instrumentId,
        String uploadedAt
) {
    public static DocumentMetadataResponse from(AdditionalDocument doc) {
        return new DocumentMetadataResponse(
                doc.getId(),
                doc.getFilename(),
                doc.getContentType(),
                doc.getSha256Hash(),
                doc.getSizeBytes(),
                doc.getInstrumentId() != null ? doc.getInstrumentId() : "",
                doc.getUploadedAt().toString()
        );
    }
}
