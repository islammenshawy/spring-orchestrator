package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Discovers, orders, and indexes all step handlers.
 * Supports parallel groups: multiple steps at the same order with @Parallel.
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
        log.info("Step registry: {}", orderedStepNames);
    }

    public StepHandler<F> getHandler(String stepName) {
        StepHandler<F> handler = steps.get(stepName);
        if (handler == null) {
            throw new IllegalArgumentException("No handler for step: " + stepName);
        }
        return handler;
    }

    public String getFirstStep() {
        return orderedStepNames.get(0);
    }

    /**
     * Returns the next step(s) after the current step.
     * For sequential steps: returns a single step name.
     * For parallel groups: returns null (parallel steps are published separately).
     */
    public String getNextStep(String currentStep) {
        StepHandler<F> current = steps.get(currentStep);
        if (current == null) return null;

        int currentOrder = current.getOrder();

        // Find all steps with the next order number
        List<StepHandler<F>> nextSteps = steps.values().stream()
                .filter(h -> h.getOrder() > currentOrder)
                .collect(Collectors.toList());

        if (nextSteps.isEmpty()) return null;

        // Get the minimum next order
        int nextOrder = nextSteps.stream()
                .mapToInt(StepHandler::getOrder)
                .min().orElse(-1);

        // If the current step is parallel, other parallel steps at the same order
        // might not be done yet — let the orchestrator handle that
        List<StepHandler<F>> atNextOrder = nextSteps.stream()
                .filter(h -> h.getOrder() == nextOrder)
                .collect(Collectors.toList());

        // Return the first one — orchestrator checks for parallel groups
        return atNextOrder.get(0).getStepName();
    }

    /**
     * Returns all steps in a parallel group.
     */
    public List<StepHandler<F>> getParallelGroup(String groupName) {
        return steps.values().stream()
                .filter(h -> h instanceof MethodStepAdapter<?> adapter && adapter.isParallel()
                        && groupName.equals(adapter.getParallelGroup()))
                .collect(Collectors.toList());
    }

    /**
     * Returns all step names at the same order (parallel siblings).
     */
    public List<String> getStepsAtSameOrder(String stepName) {
        StepHandler<F> step = steps.get(stepName);
        if (step == null) return List.of(stepName);

        int order = step.getOrder();
        return steps.values().stream()
                .filter(h -> h.getOrder() == order)
                .map(StepHandler::getStepName)
                .collect(Collectors.toList());
    }

    public boolean isLastStep(String stepName) {
        return orderedStepNames.indexOf(stepName) == orderedStepNames.size() - 1;
    }

    public List<String> getStepNames() {
        return orderedStepNames;
    }

    public List<String> getCompletedStepsBefore(String currentStep) {
        int idx = orderedStepNames.indexOf(currentStep);
        if (idx <= 0) return List.of();
        return orderedStepNames.subList(0, idx);
    }
}
