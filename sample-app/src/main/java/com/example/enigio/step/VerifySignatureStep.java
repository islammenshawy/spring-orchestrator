package com.example.enigio.step;

import com.example.enigio.flow.EnigioFlow;
import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.StepHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Slf4j
@Component
@Step(name = "VERIFY_SIGNATURE", order = 4)
@RetryOn(httpStatus = {500, 502, 503})
@FailOn(httpStatus = {400, 403})
public class VerifySignatureStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public VerifySignatureStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return false; // Always poll — verification is stateless
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Verifying signature for flow {}", flow.getId());
        Map response = client.get()
                .uri("/documents/{id}/signature-status?signatureRequestId={sid}",
                        flow.getEnigioDocumentId(), flow.getSignatureRequestId())
                .retrieve().bodyToMono(Map.class).block();
        if (!Boolean.TRUE.equals(response.get("verified"))) {
            // Manual throw — not an HTTP error, just "not ready yet"
            throw new RetryableStepException("Signature not yet verified");
        }
    }
}
