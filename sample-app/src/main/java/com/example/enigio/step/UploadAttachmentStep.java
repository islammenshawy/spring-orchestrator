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
@Step(name = "UPLOAD_ATTACHMENT", order = 2)
@RetryOn(httpStatus = {500, 502, 503, 429})
@RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
@FailOn(httpStatus = {400, 403})
public class UploadAttachmentStep implements StepHandler<EnigioFlow> {

    private final WebClient client;

    public UploadAttachmentStep(@Value("${vendor.base-url}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public boolean isAlreadyCompleted(EnigioFlow flow) {
        return flow.getAttachmentId() != null;
    }

    @Override
    public void execute(EnigioFlow flow) {
        log.info("Uploading attachment for flow {}", flow.getId());
        Map response = client.post().uri("/documents/{id}/attachments", flow.getEnigioDocumentId())
                .bodyValue(Map.of("fileName", "doc.pdf", "fileContent", flow.getContent()))
                .retrieve().bodyToMono(Map.class).block();
        flow.setAttachmentId((String) response.get("attachmentId"));
    }
}
