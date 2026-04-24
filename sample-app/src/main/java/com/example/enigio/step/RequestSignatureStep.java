package com.example.enigio.step;

import com.example.enigio.flow.EnigioFlow;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.StepHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.Map;

@Slf4j
@Component
public class RequestSignatureStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public RequestSignatureStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override public String getStepName() { return "REQUEST_SIGNATURE"; }
    @Override public int getOrder() { return 3; }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getSignatureRequestId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Requesting signature for flow {}", flow.getId());
        try {
            Map response = client.post().uri("/documents/{id}/sign", flow.getEnigioDocumentId())
                    .bodyValue(Map.of("signerEmail", flow.getSignerEmail()))
                    .retrieve().bodyToMono(Map.class).block();
            flow.setSignatureRequestId((String) response.get("signatureRequestId"));
        } catch (WebClientResponseException e) {
            throw new RetryableStepException("Vendor error: " + e.getStatusCode(), e);
        }
    }
}
