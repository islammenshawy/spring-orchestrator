package com.enigio.orchestrator.si.handler;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.FinalizeDocumentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FinalizeDocumentHandler {

    private final EnigioClient enigioClient;

    public DocumentFlow handle(DocumentFlow flow) {
        if (flow.getFinalDocumentUrl() != null) {
            log.info("[SI] FINALIZE_DOCUMENT already done for flow {}, skipping", flow.getId());
            return flow;
        }
        log.info("[SI] Executing FINALIZE_DOCUMENT for flow {}", flow.getId());
        FinalizeDocumentResponse response = enigioClient.finalizeDocument(flow.getEnigioDocumentId());
        flow.setFinalDocumentUrl(response.getFinalDocumentUrl());
        flow.setTraceHash(response.getTraceHash());
        return flow;
    }
}
