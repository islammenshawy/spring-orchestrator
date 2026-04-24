package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Discovers and orders all StepHandler beans at startup.
 * Steps are sorted by getOrder() and indexed by getStepName().
 * Provides next-step lookup for the orchestrator.
 */
@Slf4j
public class StepRegistry<F extends OrchestratorFlow> {

    private final LinkedHashMap<String, StepHandler<F>> steps;
    private final List<String> orderedStepNames;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public StepRegistry(List handlers) {
        this.steps = new LinkedHashMap<>();
        List<StepHandler<F>> typed = (List<StepHandler<F>>) handlers;
        typed.stream()
                .sorted(Comparator.comparingInt(StepHandler::getOrder))
                .forEach(h -> steps.put(h.getStepName(), h));

        this.orderedStepNames = List.copyOf(steps.keySet());
        log.info("Orchestrator step registry: {}", orderedStepNames);
    }

    public StepHandler<F> getHandler(String stepName) {
        StepHandler<F> handler = steps.get(stepName);
        if (handler == null) {
            throw new IllegalArgumentException("No handler registered for step: " + stepName);
        }
        return handler;
    }

    public String getFirstStep() {
        return orderedStepNames.get(0);
    }

    public String getNextStep(String currentStep) {
        int idx = orderedStepNames.indexOf(currentStep);
        if (idx < 0 || idx >= orderedStepNames.size() - 1) {
            return null; // last step or not found
        }
        return orderedStepNames.get(idx + 1);
    }

    public boolean isLastStep(String stepName) {
        return orderedStepNames.indexOf(stepName) == orderedStepNames.size() - 1;
    }

    public List<String> getStepNames() {
        return orderedStepNames;
    }
}
