package com.enigio.orchestrator.saga.saga;

import com.enigio.orchestrator.common.domain.FlowStep;
import com.enigio.orchestrator.saga.saga.steps.SagaStep;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SagaStepRegistry {

    private final Map<FlowStep, SagaStep> steps;

    public SagaStepRegistry(List<SagaStep> sagaSteps) {
        this.steps = sagaSteps.stream()
                .collect(Collectors.toMap(SagaStep::getStepName, Function.identity()));
    }

    public SagaStep getStep(FlowStep flowStep) {
        SagaStep step = steps.get(flowStep);
        if (step == null) {
            throw new IllegalArgumentException("No saga step registered for: " + flowStep);
        }
        return step;
    }
}
