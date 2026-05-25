package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.kafka.StepCommandMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

/**
 * Re-activates a parked flow by publishing a step command to Kafka.
 * Shared by webhook event handlers that need to wake up waiting flows.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FlowReactivator {

    private final MongoTemplate mongoTemplate;
    @SuppressWarnings("rawtypes")
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    @Value("${orchestrator.kafka.command-topic:dis.instrument.commands}")
    private String commandTopic;

    @SuppressWarnings("unchecked")
    public void reactivate(String flowId, String stepName) {
        EnigioInstrumentEntity entity = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
        String partitionKey = (entity != null && entity.getCorrelationId() != null)
                ? entity.getCorrelationId() : flowId;
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .correlationId(partitionKey)
                    .stepName(stepName)
                    .flowType("enigio-instrument")
                    .build();
            kafkaTemplate.send(commandTopic, partitionKey, objectMapper.writeValueAsString(cmd)).get();
            log.info("[webhook] Re-activated flow {} at step {}", flowId, stepName);
        } catch (Exception e) {
            log.error("[webhook] Failed to re-activate flow {}: {}", flowId, e.getMessage());
        }
    }
}
