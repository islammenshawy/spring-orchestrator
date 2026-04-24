package com.enigio.orchestrator.common.kafka;

import com.enigio.orchestrator.common.domain.FlowStep;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlowCommandMessage {
    private String eventId;
    private String flowId;
    private String correlationId;
    private FlowStep step;
    private String payload;
}
