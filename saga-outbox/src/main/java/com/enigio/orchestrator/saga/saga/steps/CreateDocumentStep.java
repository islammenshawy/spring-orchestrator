package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.CreateDocumentRequest;
import com.enigio.orchestrator.common.client.dto.CreateDocumentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateDocumentStep implements SagaStep {

    private final EnigioClient enigioClient;

    @Override
    public FlowStep getStepName() {
        return FlowStep.CREATE_DOCUMENT;
    }

    @Override
    public DocumentFlow execute(DocumentFlow flow) {
        // Idempotency guard: if we already have a documentId, this step
        // already succeeded on a previous attempt (container crashed after
        // API call but before persisting to DB). Skip the API call.
        if (flow.getEnigioDocumentId() != null) {
            log.info("CREATE_DOCUMENT already completed for flow {} (docId={}), skipping",
                    flow.getId(), flow.getEnigioDocumentId());
            return flow;
        }

        log.info("Executing CREATE_DOCUMENT for flow {}", flow.getId());
        CreateDocumentResponse response = enigioClient.createDocument(
                CreateDocumentRequest.builder()
                        .title(flow.getTitle())
                        .content(flow.getContent())
                        .metadata(flow.getMetadata())
                        .build());
        flow.setEnigioDocumentId(response.getDocumentId());
        return flow;
    }

    @Override
    public DocumentFlow compensate(DocumentFlow flow) {
        log.info("Compensating CREATE_DOCUMENT for flow {} (no-op, document will expire)", flow.getId());
        return flow;
    }
}
