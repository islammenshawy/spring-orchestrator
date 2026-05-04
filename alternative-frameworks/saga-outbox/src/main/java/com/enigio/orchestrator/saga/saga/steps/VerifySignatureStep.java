package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.VerifySignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;
import com.enigio.orchestrator.common.exception.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VerifySignatureStep implements SagaStep {

    private final EnigioClient enigioClient;

    @Override
    public FlowStep getStepName() {
        return FlowStep.VERIFY_SIGNATURE;
    }

    @Override
    public DocumentFlow execute(DocumentFlow flow) {
        log.info("Executing VERIFY_SIGNATURE for flow {}", flow.getId());
        VerifySignatureResponse response = enigioClient.verifySignature(
                flow.getEnigioDocumentId(),
                flow.getSignatureRequestId());

        if (!response.isVerified()) {
            throw new RetryableException("Signature not yet verified for flow " + flow.getId());
        }
        return flow;
    }

    @Override
    public DocumentFlow compensate(DocumentFlow flow) {
        log.info("Compensating VERIFY_SIGNATURE for flow {} (no-op)", flow.getId());
        return flow;
    }
}
