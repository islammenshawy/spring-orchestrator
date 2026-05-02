package com.dis.instrument.vendor.enigio;

import com.dis.instrument.vendor.enigio.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Pass-through endpoint for downstream systems to query Enigio vendor state directly.
 * Useful for sync/reconciliation when the orchestrator's local state may be stale.
 */
@Slf4j
@RestController
@RequestMapping("/vendor/enigio")
@Tag(name = "Vendor Sync", description = "Read-only pass-through to Enigio trace:original (v3.3). "
        + "Use to reconcile local orchestrator state against the vendor ledger — "
        + "e.g. after a crash, timeout, missed webhook, or when the orchestrator status looks inconsistent. "
        + "All sections are fetched in parallel for minimal latency.")
public class EnigioSyncController {

    private static final Set<String> VALID_INCLUDES = Set.of(
            "document", "metadata", "technicalDetails", "requiredSignatures");

    private final EnigioClient enigioClient;
    private final ExecutorService executor;

    public EnigioSyncController(EnigioClient enigioClient) {
        this.enigioClient = enigioClient;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @PreDestroy
    void shutdown() {
        executor.close();
    }

    @Operation(
            summary = "Get vendor state for a document",
            description = """
                    Fetches selected sections of document state from Enigio trace:original in parallel.
                    Use the `include` query parameter to request only what you need.

                    **Defaults** to `metadata` + `technicalDetails` (lightweight reconciliation).

                    **Typical use cases:**
                    - **Crash recovery:** fetch `technicalDetails` to get the real `versionKey` after a crash between vendor call and local DB write
                    - **Missed webhook:** fetch `requiredSignatures` to verify actual signing status on the vendor side
                    - **Invalidation audit:** fetch `metadata` to check `inTransit` / `invalidated` flags
                    - **Content verification:** fetch `document` to pull base-64 content before approving the signing phase

                    Individual section failures are returned inline as errors — the response always includes all requested sections.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Vendor state returned — only requested sections are populated, others are null",
                            content = @Content(schema = @Schema(implementation = VendorSyncResponse.class))),
                    @ApiResponse(responseCode = "400", description = "Bad request — invalid traceOriginalId or vendor-side validation error (Enigio code 3500)",
                            content = @Content(schema = @Schema(implementation = VendorErrorResponse.class))),
                    @ApiResponse(responseCode = "404", description = "Document not found on the Enigio ledger",
                            content = @Content(schema = @Schema(implementation = VendorErrorResponse.class))),
                    @ApiResponse(responseCode = "502", description = "Vendor unreachable — Enigio API returned 5xx or connection timed out")
            })
    @GetMapping("/documents/{traceOriginalId}")
    public ResponseEntity<?> getDocumentState(
            @Parameter(description = "Enigio trace:original document ID",
                    example = "60fcd0a7d84b1b5df0542b29b7a941abe6b45b20de7585ff2e91cbaf1665dac5")
            @PathVariable String traceOriginalId,

            @Parameter(description = "Sections to include (comma-separated). "
                    + "Valid values: `document`, `metadata`, `technicalDetails`, `requiredSignatures`. "
                    + "Defaults to `metadata,technicalDetails` if omitted.",
                    example = "metadata,technicalDetails,requiredSignatures")
            @RequestParam(required = false) Set<String> include) {

        Set<String> sections = (include == null || include.isEmpty())
                ? Set.of("metadata", "technicalDetails")
                : include;

        log.info("Sync: fetching {} for {}", sections, traceOriginalId);

        // Fan out all requested sections in parallel on virtual threads
        CompletableFuture<VendorDocumentResponse> docFuture = null;
        CompletableFuture<VendorDocumentMetadata> metaFuture = null;
        CompletableFuture<VendorTechnicalDetails> techFuture = null;
        CompletableFuture<List<VendorRequiredSignature>> sigFuture = null;

        for (String section : sections) {
            if (!VALID_INCLUDES.contains(section)) {
                continue;
            }
            switch (section) {
                case "document" -> docFuture =
                        CompletableFuture.supplyAsync(() -> enigioClient.getDocument(traceOriginalId), executor);
                case "metadata" -> metaFuture =
                        CompletableFuture.supplyAsync(() -> enigioClient.getDocumentMetadata(traceOriginalId), executor);
                case "technicalDetails" -> techFuture =
                        CompletableFuture.supplyAsync(() -> enigioClient.getTechnicalDetails(traceOriginalId), executor);
                case "requiredSignatures" -> sigFuture =
                        CompletableFuture.supplyAsync(() -> enigioClient.getRequiredSignatures(traceOriginalId), executor);
            }
        }

        // Wait for all futures — individual failures don't block others
        var allFutures = new ArrayList<CompletableFuture<?>>();
        if (docFuture != null) allFutures.add(docFuture);
        if (metaFuture != null) allFutures.add(metaFuture);
        if (techFuture != null) allFutures.add(techFuture);
        if (sigFuture != null) allFutures.add(sigFuture);

        try {
            CompletableFuture.allOf(allFutures.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException e) {
            log.warn("Sync: partial failure fetching vendor state for {}: {}", traceOriginalId, e.getMessage());
            return ResponseEntity.status(502).body(Map.of(
                    "error", "Vendor call failed",
                    "traceOriginalId", traceOriginalId,
                    "message", e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
            ));
        }

        var response = new VendorSyncResponse(
                traceOriginalId,
                docFuture != null ? docFuture.join() : null,
                metaFuture != null ? metaFuture.join() : null,
                techFuture != null ? techFuture.join() : null,
                sigFuture != null ? sigFuture.join() : null
        );

        return ResponseEntity.ok(response);
    }
}
