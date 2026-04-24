package com.enigio.orchestrator.si.handler;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentRequest;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UploadAttachmentHandler {

    private final EnigioClient enigioClient;

    public DocumentFlow handle(DocumentFlow flow) {
        if (flow.getAttachmentId() != null) {
            log.info("[SI] UPLOAD_ATTACHMENT already done for flow {}, skipping", flow.getId());
            return flow;
        }
        log.info("[SI] Executing UPLOAD_ATTACHMENT for flow {}", flow.getId());
        UploadAttachmentResponse response = enigioClient.uploadAttachment(
                flow.getEnigioDocumentId(),
                UploadAttachmentRequest.builder()
                        .fileName("document.pdf")
                        .fileContent(flow.getContent())
                        .build());
        flow.setAttachmentId(response.getAttachmentId());
        return flow;
    }
}
