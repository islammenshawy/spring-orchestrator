package com.dis.instrument.vendor.enigio.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Maps to Enigio TechnicalDetailsDTO (v3.3).
 */
@Schema(description = "Latest technical details from Enigio trace:original ledger")
public record VendorTechnicalDetails(

        @Schema(description = "The trace:original ID",
                example = "083fde1f81275c0bca5a5e6fcc3eba0f2c91be29ad6939bc2b57fcb35c0dcc35")
        String traceOriginalId,

        @Schema(description = "Current version key — required for mutation operations (amend, invalidate, transfer)",
                example = "d515e3e9b01ede88637993772fa7916bdae79dd4b959c77cd77ba179e629bdc1")
        String versionKey,

        @Schema(description = "SHA-256 hash of the document content",
                example = "b84667575710aaa1c95b4085cd45db79a454fd890dbbfe52745b16602008ce75")
        String contentHash,

        @Schema(description = "ISO-8601 timestamp when this version was created", example = "2024-04-03T13:27:00.901Z")
        String documentTimestamp,

        @Schema(description = "Public key of the document owner",
                example = "02fc11cf8122e80e12eefe06f58a4e65e4091b5b268d1fa171ea16a1039ab0e9e1")
        String ownerKey,

        @Schema(description = "URL for public notary verification", example = "https://www.traceoriginal.com/")
        String ledgerUrl
) {}
