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
public class FinalizeDocumentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public FinalizeDocumentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override public String getStepName() { return "FINALIZE_DOCUMENT"; }
    @Override public int getOrder() { return 5; }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getFinalDocumentUrl() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Finalizing document for flow {}", flow.getId());
        try {
            Map response = client.post().uri("/documents/{id}/finalize", flow.getEnigioDocumentId())
                    .retrieve().bodyToMono(Map.class).block();
            flow.setFinalDocumentUrl((String) response.get("finalDocumentUrl"));
            flow.setTraceHash((String) response.get("traceHash"));
        } catch (WebClientResponseException e) {
            throw new RetryableStepException("Vendor error: " + e.getStatusCode(), e);
        }
    }
}
