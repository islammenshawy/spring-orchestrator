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
public class UploadAttachmentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public UploadAttachmentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override public String getStepName() { return "UPLOAD_ATTACHMENT"; }
    @Override public int getOrder() { return 2; }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getAttachmentId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Uploading attachment for flow {}", flow.getId());
        try {
            Map response = client.post().uri("/documents/{id}/attachments", flow.getEnigioDocumentId())
                    .bodyValue(Map.of("fileName", "doc.pdf", "fileContent", flow.getContent()))
                    .retrieve().bodyToMono(Map.class).block();
            flow.setAttachmentId((String) response.get("attachmentId"));
        } catch (WebClientResponseException e) {
            throw new RetryableStepException("Vendor error: " + e.getStatusCode(), e);
        }
    }
}
