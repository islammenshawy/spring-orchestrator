package com.dis.instrument.vendor.enigio.feign;

import com.dis.instrument.vendor.enigio.dto.*;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

/**
 * Feign client for Enigio trace:original Document APIs.
 */
@FeignClient(name = "enigio-documents", url = "${vendor.enigio.base-url}",
        configuration = EnigioFeignConfig.class)
public interface EnigioDocumentClient {

    @PostMapping("/documents")
    DocumentOperationResponse createDocument(@RequestBody CreateDocumentRequest request);

    @PostMapping("/documents/{id}/amend")
    DocumentOperationResponse amendDocument(@PathVariable("id") String traceOriginalId,
                                             @RequestBody AmendDocumentRequest request);

    @PostMapping("/documents/{id}/invalidate")
    void invalidateDocument(@PathVariable("id") String traceOriginalId,
                            @RequestBody InvalidateRequest request);

    @PostMapping("/documents/validate")
    ValidateResponse validateDocument(@RequestBody ValidateRequest request);

    @GetMapping("/documents/{id}")
    com.dis.instrument.vendor.enigio.dto.VendorDocumentResponse getDocument(
            @PathVariable("id") String traceOriginalId);

    @GetMapping("/documents/{id}/metadata")
    com.dis.instrument.vendor.enigio.dto.VendorDocumentMetadata getDocumentMetadata(
            @PathVariable("id") String traceOriginalId);

    @GetMapping("/documents/{id}/technical-details/latest")
    com.dis.instrument.vendor.enigio.dto.VendorTechnicalDetails getTechnicalDetails(
            @PathVariable("id") String traceOriginalId);
}
