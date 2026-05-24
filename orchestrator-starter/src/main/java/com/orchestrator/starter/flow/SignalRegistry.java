package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry of @Signal handlers for a flow type.
 * Discovered at startup by FlowDefinitionScanner.
 */
public class SignalRegistry<F extends OrchestratorFlow> {

    private final Map<String, SignalHandler<F>> handlers = new LinkedHashMap<>();

    public void register(String signalName, SignalHandler<F> handler) {
        handlers.put(signalName, handler);
    }

    public SignalHandler<F> getHandler(String signalName) {
        return handlers.get(signalName);
    }

    public Set<String> getSignalNames() {
        return handlers.keySet();
    }

    public boolean isEmpty() {
        return handlers.isEmpty();
    }
}
