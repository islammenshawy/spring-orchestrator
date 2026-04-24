package com.enigio.orchestrator.si.handler;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.RequestSignatureRequest;
import com.enigio.orchestrator.common.client.dto.RequestSignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestSignatureHandler {

    private final EnigioClient enigioClient;

    public DocumentFlow handle(DocumentFlow flow) {
        if (flow.getSignatureRequestId() != null) {
            log.info("[SI] REQUEST_SIGNATURE already done for flow {}, skipping", flow.getId());
            return flow;
        }
        log.info("[SI] Executing REQUEST_SIGNATURE for flow {}", flow.getId());
        RequestSignatureResponse response = enigioClient.requestSignature(
                flow.getEnigioDocumentId(),
                RequestSignatureRequest.builder()
                        .signerEmail(flow.getSignerEmail())
                        .build());
        flow.setSignatureRequestId(response.getSignatureRequestId());
        return flow;
    }
}
