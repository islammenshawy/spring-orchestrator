package com.enigio.orchestrator.si.handler;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.CreateDocumentRequest;
import com.enigio.orchestrator.common.client.dto.CreateDocumentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateDocumentHandler {

    private final EnigioClient enigioClient;

    public DocumentFlow handle(DocumentFlow flow) {
        if (flow.getEnigioDocumentId() != null) {
            log.info("[SI] CREATE_DOCUMENT already done for flow {}, skipping", flow.getId());
            return flow;
        }
        log.info("[SI] Executing CREATE_DOCUMENT for flow {}", flow.getId());
        CreateDocumentResponse response = enigioClient.createDocument(
                CreateDocumentRequest.builder()
                        .title(flow.getTitle())
                        .content(flow.getContent())
                        .metadata(flow.getMetadata())
                        .build());
        flow.setEnigioDocumentId(response.getDocumentId());
        return flow;
    }
}
