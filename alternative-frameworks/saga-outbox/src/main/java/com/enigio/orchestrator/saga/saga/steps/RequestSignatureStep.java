package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.RequestSignatureRequest;
import com.enigio.orchestrator.common.client.dto.RequestSignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestSignatureStep implements SagaStep {

    private final EnigioClient enigioClient;

    @Override
    public FlowStep getStepName() {
        return FlowStep.REQUEST_SIGNATURE;
    }

    @Override
    public DocumentFlow execute(DocumentFlow flow) {
        if (flow.getSignatureRequestId() != null) {
            log.info("REQUEST_SIGNATURE already completed for flow {} (sigReqId={}), skipping",
                    flow.getId(), flow.getSignatureRequestId());
            return flow;
        }

        log.info("Executing REQUEST_SIGNATURE for flow {}", flow.getId());
        RequestSignatureResponse response = enigioClient.requestSignature(
                flow.getEnigioDocumentId(),
                RequestSignatureRequest.builder()
                        .signerEmail(flow.getSignerEmail())
                        .build());
        flow.setSignatureRequestId(response.getSignatureRequestId());
        return flow;
    }

    @Override
    public DocumentFlow compensate(DocumentFlow flow) {
        log.info("Compensating REQUEST_SIGNATURE for flow {} (cancel signature request)", flow.getId());
        return flow;
    }
}
