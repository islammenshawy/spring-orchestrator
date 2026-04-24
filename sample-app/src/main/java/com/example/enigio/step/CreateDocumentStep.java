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
public class CreateDocumentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public CreateDocumentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override public String getStepName() { return "CREATE_DOCUMENT"; }
    @Override public int getOrder() { return 1; }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getEnigioDocumentId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Creating document for flow {}", flow.getId());
        try {
            Map response = client.post().uri("/documents")
                    .bodyValue(Map.of("title", flow.getTitle(), "content", flow.getContent()))
                    .retrieve().bodyToMono(Map.class).block();
            flow.setEnigioDocumentId((String) response.get("documentId"));
        } catch (WebClientResponseException e) {
            throw new RetryableStepException("Vendor error: " + e.getStatusCode(), e);
        }
    }
}
