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
@Step(name = "FINALIZE_DOCUMENT", order = 5)
@RetryOn(httpStatus = {500, 502, 503, 429})
@RecoverOn(httpStatus = 409, message = "already finalized", action = RecoverAction.SKIP)
@FailOn(httpStatus = {400, 403})
public class FinalizeDocumentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public FinalizeDocumentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getFinalDocumentUrl() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Finalizing document for flow {}", flow.getId());
        Map response = client.post().uri("/documents/{id}/finalize", flow.getEnigioDocumentId())
                .retrieve().bodyToMono(Map.class).block();
        flow.setFinalDocumentUrl((String) response.get("finalDocumentUrl"));
        flow.setTraceHash((String) response.get("traceHash"));
    }
}
