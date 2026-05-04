package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.FinalizeDocumentResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FinalizeDocumentStep implements SagaStep {

    private final EnigioClient enigioClient;

    @Override
    public FlowStep getStepName() {
        return FlowStep.FINALIZE_DOCUMENT;
    }

    @Override
    public DocumentFlow execute(DocumentFlow flow) {
        if (flow.getFinalDocumentUrl() != null) {
            log.info("FINALIZE_DOCUMENT already completed for flow {} (url={}), skipping",
                    flow.getId(), flow.getFinalDocumentUrl());
            return flow;
        }

        log.info("Executing FINALIZE_DOCUMENT for flow {}", flow.getId());
        FinalizeDocumentResponse response = enigioClient.finalizeDocument(flow.getEnigioDocumentId());
        flow.setFinalDocumentUrl(response.getFinalDocumentUrl());
        flow.setTraceHash(response.getTraceHash());
        return flow;
    }

    @Override
    public DocumentFlow compensate(DocumentFlow flow) {
        log.info("Compensating FINALIZE_DOCUMENT for flow {} (no-op, cannot un-finalize)", flow.getId());
        return flow;
    }
}
