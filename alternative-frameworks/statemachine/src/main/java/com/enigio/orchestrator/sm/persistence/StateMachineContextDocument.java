package com.enigio.orchestrator.sm.persistence;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "state_machine_contexts")
public class StateMachineContextDocument {

    @Id
    private String machineId;
    private String state;
    private Map<String, Object> extendedState;
    @Builder.Default
    private Instant updatedAt = Instant.now();
}
