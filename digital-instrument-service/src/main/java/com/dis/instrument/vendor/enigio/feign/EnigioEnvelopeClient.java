package com.dis.instrument.vendor.enigio.feign;

import com.dis.instrument.vendor.enigio.dto.*;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

/**
 * Feign client for Enigio trace:original Envelope APIs.
 */
@FeignClient(name = "enigio-envelopes", url = "${vendor.enigio.base-url}",
        configuration = EnigioFeignConfig.class)
public interface EnigioEnvelopeClient {

    @PostMapping("/envelopes/drafts")
    EnvelopeDraftResponse createEnvelopeDraft(@RequestBody EnvelopeDraftRequest request);

    @PostMapping("/envelopes/drafts/{draftId}/additional-documents")
    FileUploadResponse uploadAdditionalDocument(
            @PathVariable("draftId") String draftId,
            @RequestHeader("File-Name") String filename,
            @RequestHeader("File-Hash") String sha256,
            @RequestBody byte[] data);

    @PostMapping("/envelopes/drafts/{draftId}/seal")
    DocumentOperationResponse sealEnvelopeDraft(@PathVariable("draftId") String draftId);

    @PostMapping("/envelopes/{id}/transfer-by-email")
    TransferResponse transferByEmail(@PathVariable("id") String envelopeTraceId,
                                      @RequestBody TransferRequest request);

    @DeleteMapping("/envelopes/{transferId}/transfer-by-email")
    void cancelEnvelopeTransfer(@PathVariable("transferId") String transferId);
}
