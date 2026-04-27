package com.orchestrator.starter.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StepCommandMessage {
    private String eventId;
    private String flowId;
    private String correlationId;
    private String stepName;
    private String flowType;
}
