package com.enigio.orchestrator.sm.listeners;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import com.enigio.orchestrator.sm.persistence.SmStepLog;
import com.enigio.orchestrator.sm.persistence.SmStepLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.statemachine.state.State;
import org.springframework.statemachine.transition.Transition;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlowStateChangeListener extends StateMachineListenerAdapter<DocumentFlowStates, DocumentFlowEvents> {

    private final DocumentFlowRepository flowRepository;
    private final SmStepLogRepository stepLogRepository;

    private String flowId;

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    @Override
    public void transitionEnded(Transition<DocumentFlowStates, DocumentFlowEvents> transition) {
        if (flowId == null || transition.getTarget() == null) return;

        State<DocumentFlowStates, DocumentFlowEvents> source = transition.getSource();
        State<DocumentFlowStates, DocumentFlowEvents> target = transition.getTarget();

        String fromState = source != null ? source.getId().name() : "NONE";
        String toState = target.getId().name();

        log.info("[SM] Flow {} transitioned: {} -> {}", flowId, fromState, toState);

        // Log the transition
        SmStepLog stepLog = SmStepLog.builder()
                .id(UUID.randomUUID().toString())
                .flowId(flowId)
                .fromState(fromState)
                .toState(toState)
                .event(transition.getTrigger() != null ? transition.getTrigger().getEvent().name() : null)
                .status(toState.equals("FAILED") ? "FAILED" : "COMPLETED")
                .build();
        stepLogRepository.save(stepLog);

        // Update the DocumentFlow
        flowRepository.findById(flowId).ifPresent(flow -> {
            if (target.getId() == DocumentFlowStates.COMPLETED) {
                flow.setStatus(FlowStatus.COMPLETED);
            } else if (target.getId() == DocumentFlowStates.FAILED) {
                flow.setStatus(FlowStatus.FAILED);
            } else {
                flow.setStatus(FlowStatus.IN_PROGRESS);
            }
            flow.setUpdatedAt(Instant.now());
            flowRepository.save(flow);
        });
    }
}
