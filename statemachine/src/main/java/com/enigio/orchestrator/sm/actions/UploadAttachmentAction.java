package com.enigio.orchestrator.sm.actions;

import com.enigio.orchestrator.common.client.EnigioClient;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentRequest;
import com.enigio.orchestrator.common.client.dto.UploadAttachmentResponse;
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
public class UploadAttachmentAction implements Action<DocumentFlowStates, DocumentFlowEvents> {

    private final EnigioClient enigioClient;
    private final DocumentFlowRepository flowRepository;

    @Override
    public void execute(StateContext<DocumentFlowStates, DocumentFlowEvents> context) {
        String flowId = (String) context.getExtendedState().getVariables().get("flowId");
        DocumentFlow flow = flowRepository.findById(flowId).orElseThrow();

        if (flow.getAttachmentId() != null) {
            log.info("[SM] UPLOAD_ATTACHMENT already done for flow {}, skipping", flowId);
            context.getExtendedState().getVariables().put("stepCompleted", true);
            return;
        }

        log.info("[SM] Executing UPLOAD_ATTACHMENT for flow {}", flowId);
        UploadAttachmentResponse response = enigioClient.uploadAttachment(
                flow.getEnigioDocumentId(),
                UploadAttachmentRequest.builder()
                        .fileName("document.pdf")
                        .fileContent(flow.getContent())
                        .build());

        flow.setAttachmentId(response.getAttachmentId());
        flowRepository.save(flow);
        context.getExtendedState().getVariables().put("stepCompleted", true);
    }
}
