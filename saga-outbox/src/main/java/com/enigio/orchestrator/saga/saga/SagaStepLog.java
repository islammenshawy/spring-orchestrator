package com.enigio.orchestrator.saga.saga;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "saga_step_logs")
@CompoundIndex(name = "flow_step_attempt", def = "{'flowId': 1, 'stepName': 1, 'attemptNumber': 1}", unique = true)
public class SagaStepLog {

    @Id
    private String id;
    private String flowId;
    private String stepName;
    private String status; // PENDING, EXECUTING, COMPLETED, FAILED, COMPENSATED
    private String requestPayload;
    private String responsePayload;
    private String errorMessage;
    @Builder.Default
    private int attemptNumber = 1;
    private Instant startedAt;
    private Instant completedAt;
}
