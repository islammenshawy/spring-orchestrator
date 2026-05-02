package com.dis.instrument.core.api;

import com.dis.instrument.core.model.AdditionalDocument;
import com.dis.instrument.core.model.AdditionalDocumentRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Upload additional documents to MongoDB before starting a flow, or between
 * the PREPARATION_COMPLETE notification and the delivery approval.
 *
 * <b>Why not inline in the flow request?</b>
 * Binary documents can be megabytes — too large for Kafka messages.
 * This endpoint stores them in MongoDB; the flow entity only carries document IDs.
 * During the envelope step, DIS reads from MongoDB and uploads to the Enigio draft.
 */
@Slf4j
@RestController
@RequestMapping("/documents/additional")
@RequiredArgsConstructor
@Tag(name = "Additional Documents",
        description = """
                Upload supporting documents (PDFs, compliance reports, certificates, etc.) that will be
                attached to the Enigio envelope alongside the trace:original instrument.

                **Binary storage:** Documents are stored in MongoDB. The flow entity only carries document IDs,
                keeping Kafka messages lean. During the envelope creation step, DIS reads from MongoDB and
                uploads to the Enigio draft before sealing.

                **Lifecycle:**
                ```
                1. Downstream uploads documents    → POST /documents/additional
                2. Downstream starts the flow      → POST /flows/enigio-instrument
                     { ..., "additionalDocumentIds": ["doc1_id", "doc2_id"] }
                3. Steps 1-9 execute (preparation, signing, validation)
                4. Step 10: DIS reads docs from MongoDB → uploads to Enigio draft → seals
                5. Step 11: Envelope transferred to recipient with all documents
                ```

                **Alternatively**, documents can be uploaded after the flow starts (e.g., between Gate 1 and Gate 2),
                as long as they're uploaded before delivery approval. Use the `additionalDocumentsUrl` from the
                notification payload.

                **Limits:** Enigio allows max 25 documents per envelope (originals + copies + additional combined).""")
public class AdditionalDocumentController {

    /** Max 10 MB per document (base64 string is ~33% larger than raw bytes). */
    private static final int MAX_BASE64_LENGTH = 14_000_000; // ~10 MB decoded

    private final AdditionalDocumentRepository repository;

    @Operation(
            summary = "Upload an additional document",
            description = """
                    Stores the document binary in MongoDB and returns the document ID + SHA-256 hash.
                    Use the returned `id` in the flow request's `additionalDocumentIds` array.

                    **Before flow start:** Upload all documents, collect IDs, include in `POST /flows/enigio-instrument`.

                    **After flow start:** Upload using the `additionalDocumentsUrl` from the notification payload.
                    Then link the IDs by calling `POST /documents/additional/{id}/link/{instrumentId}`
                    (or include them in the initial flow request).

                    The SHA-256 hash is computed server-side and returned for verification.
                    Enigio uses this hash to verify document integrity during envelope sealing.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Document stored",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "id": "6829a1f30000000000000042",
                                              "filename": "compliance-report-Q4.pdf",
                                              "sha256Hash": "b84667575710aaa1c95b4085cd45db79a454fd890dbbfe52745b16602008ce75",
                                              "sizeBytes": 245760,
                                              "uploadedAt": "2024-04-16T14:30:00Z"
                                            }"""))),
                    @ApiResponse(responseCode = "400", description = "Invalid request — missing filename or data, or invalid base64")
            })
    @PostMapping
    public ResponseEntity<Map<String, Object>> upload(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Document upload payload",
                    content = @Content(mediaType = "application/json",
                            examples = @ExampleObject(value = """
                                    {
                                      "filename": "compliance-report-Q4.pdf",
                                      "contentType": "application/pdf",
                                      "data": "JVBERi0xLjQKJcOkw7zDtsO... (base64)",
                                      "instrumentId": "682b3f1a0000000000000001"
                                    }""")))
            @RequestBody UploadRequest request) {
        if (request.filename() == null || request.filename().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "filename is required"));
        }
        if (request.data() == null || request.data().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "data (base64) is required"));
        }
        if (request.data().length() > MAX_BASE64_LENGTH) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Document too large — max 10 MB",
                    "maxBytes", 10_000_000));
        }

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(request.data());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "data must be valid base64"));
        }

        String sha256 = sha256Hex(decoded);

        var doc = AdditionalDocument.builder()
                .instrumentId(request.instrumentId())
                .filename(request.filename())
                .contentType(request.contentType() != null ? request.contentType() : "application/octet-stream")
                .data(request.data())
                .sha256Hash(sha256)
                .sizeBytes(decoded.length)
                .uploadedAt(Instant.now())
                .build();

        var saved = repository.save(doc);
        log.info("Additional document uploaded: id={}, filename={}, size={}bytes, sha256={}, instrumentId={}",
                saved.getId(), saved.getFilename(), saved.getSizeBytes(), sha256, saved.getInstrumentId());

        return ResponseEntity.ok(Map.of(
                "id", saved.getId(),
                "filename", saved.getFilename(),
                "sha256Hash", saved.getSha256Hash(),
                "sizeBytes", saved.getSizeBytes(),
                "uploadedAt", saved.getUploadedAt().toString()
        ));
    }

    @Operation(
            summary = "Get document metadata",
            description = "Returns metadata (filename, size, hash) without the binary content. "
                    + "Use to verify an upload or inspect what's attached before approving delivery.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Document metadata",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "id": "6829a1f30000000000000042",
                                              "filename": "compliance-report-Q4.pdf",
                                              "contentType": "application/pdf",
                                              "sha256Hash": "b84667575710aaa1c95b4085cd45db79...",
                                              "sizeBytes": 245760,
                                              "instrumentId": "682b3f1a0000000000000001",
                                              "uploadedAt": "2024-04-16T14:30:00Z"
                                            }"""))),
                    @ApiResponse(responseCode = "404", description = "Document not found")
            })
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getMetadata(
            @Parameter(description = "Additional document ID (returned from upload)") @PathVariable String id) {
        return repository.findById(id)
                .map(doc -> ResponseEntity.ok(toMetadataMap(doc)))
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(
            summary = "List additional documents for an instrument",
            description = """
                    Returns metadata (no binary) for all additional documents linked to the given instrument.
                    Use to review what will be included in the envelope before approving delivery.

                    The `instrumentId` is the same value from the notification payload.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "List of document metadata (may be empty)")
            })
    @GetMapping("/instrument/{instrumentId}")
    public ResponseEntity<List<Map<String, Object>>> listByInstrument(
            @Parameter(description = "Instrument ID (from notification payload)", example = "682b3f1a0000000000000001")
            @PathVariable String instrumentId) {
        var docs = repository.findByInstrumentId(instrumentId);
        return ResponseEntity.ok(docs.stream().map(this::toMetadataMap).toList());
    }

    @Operation(
            summary = "Delete an additional document",
            description = """
                    Removes the document from MongoDB. Only valid before the envelope is sealed
                    (step 10). After sealing, documents are already uploaded to the Enigio envelope
                    and cannot be removed via this endpoint.

                    Use to correct mistakes before approving the delivery phase.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Document deleted",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {"id": "6829a1f30000000000000042", "status": "deleted"}"""))),
                    @ApiResponse(responseCode = "404", description = "Document not found")
            })
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, String>> delete(
            @Parameter(description = "Additional document ID") @PathVariable String id) {
        if (!repository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        repository.deleteById(id);
        log.info("Additional document deleted: id={}", id);
        return ResponseEntity.ok(Map.of("id", id, "status", "deleted"));
    }

    // --- DTOs ---

    @Schema(description = "Upload request for an additional document to be attached to the Enigio envelope")
    public record UploadRequest(
            @Schema(description = "Original filename (preserved in the Enigio envelope)", example = "compliance-report-Q4.pdf")
            String filename,

            @Schema(description = "MIME content type", example = "application/pdf",
                    allowableValues = {"application/pdf", "image/jpeg", "image/png", "application/octet-stream"})
            String contentType,

            @Schema(description = "Base-64 encoded file content")
            String data,

            @Schema(description = "Instrument ID to link this document to (optional — can be linked via flow request instead)",
                    example = "682b3f1a0000000000000001", nullable = true)
            String instrumentId
    ) {}

    // --- Helpers ---

    private Map<String, Object> toMetadataMap(AdditionalDocument doc) {
        return Map.of(
                "id", doc.getId(),
                "filename", doc.getFilename(),
                "contentType", doc.getContentType(),
                "sha256Hash", doc.getSha256Hash(),
                "sizeBytes", doc.getSizeBytes(),
                "instrumentId", doc.getInstrumentId() != null ? doc.getInstrumentId() : "",
                "uploadedAt", doc.getUploadedAt().toString()
        );
    }

    private static String sha256Hex(byte[] data) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
