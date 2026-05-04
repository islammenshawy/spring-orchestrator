package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentRequest;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UploadAttachmentStep implements SagaStep {

    private final EnigioClient enigioClient;

    @Override
    public FlowStep getStepName() {
        return FlowStep.UPLOAD_ATTACHMENT;
    }

    @Override
    public DocumentFlow execute(DocumentFlow flow) {
        if (flow.getAttachmentId() != null) {
            log.info("UPLOAD_ATTACHMENT already completed for flow {} (attachmentId={}), skipping",
                    flow.getId(), flow.getAttachmentId());
            return flow;
        }

        log.info("Executing UPLOAD_ATTACHMENT for flow {}", flow.getId());
        UploadAttachmentResponse response = enigioClient.uploadAttachment(
                flow.getEnigioDocumentId(),
                UploadAttachmentRequest.builder()
                        .fileName("document.pdf")
                        .fileContent(flow.getContent())
                        .build());
        flow.setAttachmentId(response.getAttachmentId());
        return flow;
    }

    @Override
    public DocumentFlow compensate(DocumentFlow flow) {
        log.info("Compensating UPLOAD_ATTACHMENT for flow {} (no-op)", flow.getId());
        return flow;
    }
}
