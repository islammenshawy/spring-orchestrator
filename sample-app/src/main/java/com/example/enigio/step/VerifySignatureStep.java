package com.example.enigio.step;

import com.example.enigio.flow.EnigioFlow;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.StepHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Slf4j
@Component
public class VerifySignatureStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public VerifySignatureStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override public String getStepName() { return "VERIFY_SIGNATURE"; }
    @Override public int getOrder() { return 4; }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return false; // Always check — verification is stateless
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Verifying signature for flow {}", flow.getId());
        Map response = client.get()
                .uri("/documents/{id}/signature-status?signatureRequestId={sid}",
                        flow.getEnigioDocumentId(), flow.getSignatureRequestId())
                .retrieve().bodyToMono(Map.class).block();
        if (!Boolean.TRUE.equals(response.get("verified"))) {
            throw new RetryableStepException("Signature not yet verified");
        }
    }
}
