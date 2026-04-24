package com.example.enigio.step;

import com.example.enigio.flow.EnigioFlow;
import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.flow.StepHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Slf4j
@Component
@Step(name = "REQUEST_SIGNATURE", order = 3)
@RetryOn(httpStatus = {500, 502, 503, 429})
@RecoverOn(httpStatus = 409, message = "already signed", action = RecoverAction.SKIP)
@FailOn(httpStatus = {400, 403})
public class RequestSignatureStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public RequestSignatureStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getSignatureRequestId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Requesting signature for flow {}", flow.getId());
        Map response = client.post().uri("/documents/{id}/sign", flow.getEnigioDocumentId())
                .bodyValue(Map.of("signerEmail", flow.getSignerEmail()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setSignatureRequestId((String) response.get("signatureRequestId"));
    }
}
