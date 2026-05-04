package com.enigio.orchestrator.si.handler;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.VerifySignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.exception.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VerifySignatureHandler {

    private final EnigioClient enigioClient;

    public DocumentFlow handle(DocumentFlow flow) {
        log.info("[SI] Executing VERIFY_SIGNATURE for flow {}", flow.getId());
        VerifySignatureResponse response = enigioClient.verifySignature(
                flow.getEnigioDocumentId(),
                flow.getSignatureRequestId());
        if (!response.isVerified()) {
            throw new RetryableException("Signature not yet verified for flow " + flow.getId());
        }
        return flow;
    }
}
