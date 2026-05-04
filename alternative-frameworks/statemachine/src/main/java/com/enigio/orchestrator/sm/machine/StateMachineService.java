package com.enigio.orchestrator.sm.machine;

import com.enigio.orchestrator.sm.listeners.FlowStateChangeListener;
import com.enigio.orchestrator.sm.persistence.MongoStateMachinePersist;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.StateMachineContext;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class StateMachineService {

    private final StateMachineFactory<DocumentFlowStates, DocumentFlowEvents> factory;
    private final MongoStateMachinePersist persist;
    private final FlowStateChangeListener listener;

    public StateMachine<DocumentFlowStates, DocumentFlowEvents> acquireMachine(String flowId) {
        StateMachine<DocumentFlowStates, DocumentFlowEvents> machine = factory.getStateMachine(flowId);

        // Try to restore from persistence
        try {
            StateMachineContext<DocumentFlowStates, DocumentFlowEvents> context = persist.read(flowId);
            if (context != null) {
                machine.stopReactively().block();
                machine.getStateMachineAccessor().doWithAllRegions(accessor ->
                        accessor.resetStateMachineReactively(
                                new DefaultStateMachineContext<>(context.getState(), null, null, null)
                        ).block()
                );
                // Restore extended state
                if (context.getExtendedState() != null) {
                    machine.getExtendedState().getVariables().putAll(context.getExtendedState().getVariables());
                }
                log.info("[SM] Restored machine {} to state {}", flowId, context.getState());
            }
        } catch (Exception e) {
            log.warn("[SM] Could not restore machine {}, starting fresh: {}", flowId, e.getMessage());
        }

        // Set the flow ID and add listener
        machine.getExtendedState().getVariables().put("flowId", flowId);
        listener.setFlowId(flowId);
        machine.addStateListener(listener);

        machine.startReactively().block();
        return machine;
    }

    public void releaseMachine(StateMachine<DocumentFlowStates, DocumentFlowEvents> machine, String flowId) {
        try {
            persist.write(machine.getState() != null
                    ? new DefaultStateMachineContext<>(machine.getState().getId(), null, null,
                    machine.getExtendedState())
                    : new DefaultStateMachineContext<>(DocumentFlowStates.INITIAL, null, null, null), flowId);
            machine.stopReactively().block();
        } catch (Exception e) {
            log.error("[SM] Error releasing machine {}: {}", flowId, e.getMessage());
        }
    }
}
