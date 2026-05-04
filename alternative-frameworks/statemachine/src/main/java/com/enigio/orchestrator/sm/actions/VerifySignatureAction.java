package com.enigio.orchestrator.sm.actions;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.VerifySignatureResponse;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.exception.RetryableException;
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
public class VerifySignatureAction implements Action<DocumentFlowStates, DocumentFlowEvents> {

    private final EnigioClient enigioClient;
    private final DocumentFlowRepository flowRepository;

    @Override
    public void execute(StateContext<DocumentFlowStates, DocumentFlowEvents> context) {
        String flowId = (String) context.getExtendedState().getVariables().get("flowId");
        DocumentFlow flow = flowRepository.findById(flowId).orElseThrow();

        log.info("[SM] Executing VERIFY_SIGNATURE for flow {}", flowId);
        VerifySignatureResponse response = enigioClient.verifySignature(
                flow.getEnigioDocumentId(),
                flow.getSignatureRequestId());

        if (!response.isVerified()) {
            throw new RetryableException("Signature not yet verified for flow " + flowId);
        }
        context.getExtendedState().getVariables().put("stepCompleted", true);
    }
}
