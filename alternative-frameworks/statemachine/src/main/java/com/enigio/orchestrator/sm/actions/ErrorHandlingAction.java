package com.enigio.orchestrator.sm.actions;

import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.action.Action;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ErrorHandlingAction implements Action<DocumentFlowStates, DocumentFlowEvents> {

    @Override
    public void execute(StateContext<DocumentFlowStates, DocumentFlowEvents> context) {
        Exception exception = context.getException();
        String flowId = (String) context.getExtendedState().getVariables().get("flowId");

        if (exception != null) {
            log.error("[SM] Error in state {} for flow {}: {}",
                    context.getSource() != null ? context.getSource().getId() : "unknown",
                    flowId,
                    exception.getMessage());
            context.getExtendedState().getVariables().put("error", exception.getMessage());
        }
    }
}
