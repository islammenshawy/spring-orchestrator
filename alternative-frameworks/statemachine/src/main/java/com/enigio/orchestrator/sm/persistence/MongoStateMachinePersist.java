package com.enigio.orchestrator.sm.persistence;

import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachineContext;
import org.springframework.statemachine.StateMachinePersist;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class MongoStateMachinePersist implements StateMachinePersist<DocumentFlowStates, DocumentFlowEvents, String> {

    private final StateMachineContextRepository repository;

    @Override
    public void write(StateMachineContext<DocumentFlowStates, DocumentFlowEvents> context, String machineId) {
        Map<String, Object> extendedState = new HashMap<>();
        context.getExtendedState().getVariables().forEach((k, v) -> extendedState.put(String.valueOf(k), v));

        StateMachineContextDocument doc = StateMachineContextDocument.builder()
                .machineId(machineId)
                .state(context.getState().name())
                .extendedState(extendedState)
                .updatedAt(Instant.now())
                .build();

        repository.save(doc);
        log.debug("[SM] Persisted state machine {} in state {}", machineId, context.getState());
    }

    @Override
    public StateMachineContext<DocumentFlowStates, DocumentFlowEvents> read(String machineId) {
        return repository.findById(machineId)
                .map(doc -> {
                    DocumentFlowStates state = DocumentFlowStates.valueOf(doc.getState());
                    DefaultStateMachineContext<DocumentFlowStates, DocumentFlowEvents> context =
                            new DefaultStateMachineContext<>(state, null, null, null);
                    if (doc.getExtendedState() != null) {
                        context.getExtendedState().getVariables().putAll(doc.getExtendedState());
                    }
                    return (StateMachineContext<DocumentFlowStates, DocumentFlowEvents>) context;
                })
                .orElse(null);
    }
}
