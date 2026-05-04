package com.enigio.orchestrator.sm.actions;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.RequestSignatureRequest;
import com.enigio.orchestrator.common.client.dto.RequestSignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.action.Action;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestSignatureAction implements Action<DocumentFlowStates, DocumentFlowEvents> {

    private final EnigioClient enigioClient;
    private final DocumentFlowRepository flowRepository;

    @Override
    public void execute(StateContext<DocumentFlowStates, DocumentFlowEvents> context) {
        String flowId = (String) context.getExtendedState().getVariables().get("flowId");
        DocumentFlow flow = flowRepository.findById(flowId).orElseThrow();

        if (flow.getSignatureRequestId() != null) {
            log.info("[SM] REQUEST_SIGNATURE already done for flow {}, skipping", flowId);
            context.getExtendedState().getVariables().put("stepCompleted", true);
            return;
        }

        log.info("[SM] Executing REQUEST_SIGNATURE for flow {}", flowId);
        RequestSignatureResponse response = enigioClient.requestSignature(
                flow.getEnigioDocumentId(),
                RequestSignatureRequest.builder()
                        .signerEmail(flow.getSignerEmail())
                        .build());

        flow.setSignatureRequestId(response.getSignatureRequestId());
        flowRepository.save(flow);
        context.getExtendedState().getVariables().put("stepCompleted", true);
    }
}
