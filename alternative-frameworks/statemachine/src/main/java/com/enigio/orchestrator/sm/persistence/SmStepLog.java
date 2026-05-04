package com.enigio.orchestrator.sm.persistence;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "sm_step_logs")
public class SmStepLog {

    @Id
    private String id;
    private String flowId;
    private String fromState;
    private String toState;
    private String event;
    private String status;
    private String errorMessage;
    @Builder.Default
    private int attemptNumber = 1;
    @Builder.Default
    private Instant createdAt = Instant.now();
}
