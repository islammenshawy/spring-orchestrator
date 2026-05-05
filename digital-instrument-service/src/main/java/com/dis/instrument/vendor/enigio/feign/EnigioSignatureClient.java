package com.dis.instrument.vendor.enigio.feign;

import com.dis.instrument.vendor.enigio.dto.VendorRequiredSignature;
import com.dis.instrument.vendor.enigio.feign.dto.*;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Feign client for Enigio trace:original Signature APIs.
 */
@FeignClient(name = "enigio-signatures", url = "${vendor.enigio.base-url}",
        configuration = EnigioFeignConfig.class)
public interface EnigioSignatureClient {

    @PostMapping("/required-signatures/original/{id}")
    void addRequiredSignatures(@PathVariable("id") String traceOriginalId,
                               @RequestBody List<Map<String, Object>> signers);

    @PostMapping("/required-signatures/send-sign-emails")
    List<String> sendSigningEmails(@RequestBody SendSigningEmailsRequest request);

    @GetMapping("/required-signatures/original/{id}/status")
    String getSigningStatus(@PathVariable("id") String traceOriginalId);

    @GetMapping("/required-signatures/original/{id}")
    List<VendorRequiredSignature> getRequiredSignatures(@PathVariable("id") String traceOriginalId);

    @PostMapping("/notifications/webhooks")
    Map<String, Object> registerWebhook(@RequestBody WebhookRegistrationRequest request);
}
