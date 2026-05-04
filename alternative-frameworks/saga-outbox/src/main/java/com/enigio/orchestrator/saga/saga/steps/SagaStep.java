package com.enigio.orchestrator.saga.saga.steps;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.FlowStep;

public interface SagaStep {

    FlowStep getStepName();

    DocumentFlow execute(DocumentFlow flow);

    DocumentFlow compensate(DocumentFlow flow);
}
