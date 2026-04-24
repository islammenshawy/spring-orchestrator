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
@Step(name = "CREATE_DOCUMENT", order = 1)
@RetryOn(httpStatus = {500, 502, 503, 429})
@RecoverOn(httpStatus = 409, message = "already", action = RecoverAction.SKIP)
@FailOn(httpStatus = {400, 403})
public class CreateDocumentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public CreateDocumentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getEnigioDocumentId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Creating document for flow {}", flow.getId());
        // No try/catch needed — @RetryOn/@RecoverOn/@FailOn handle errors
        Map response = client.post().uri("/documents")
                .bodyValue(Map.of("title", flow.getTitle(), "content", flow.getContent()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setEnigioDocumentId((String) response.get("documentId"));
    }
}
