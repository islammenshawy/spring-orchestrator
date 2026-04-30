package com.dis.instrument.core.api;

import com.dis.instrument.core.model.FlowNotification;
import com.dis.instrument.vendor.enigio.EnigioInstrumentEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

/**
 * Publishes flow progress notifications to a Kafka topic.
 * Downstream systems consume these to track progress and trigger next phases.
 */
@Slf4j
@Component
public class FlowNotificationPublisher {

    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String notificationTopic;

    @SuppressWarnings("rawtypes")
    public FlowNotificationPublisher(KafkaTemplate kafkaTemplate,
                                     ObjectMapper objectMapper,
                                     @Value("${dis.notifications.topic:dis.instrument.notifications}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.notificationTopic = topic;
    }

    @SuppressWarnings("unchecked")
    public void notifyPhaseComplete(EnigioInstrumentEntity flow, String phase, String status) {
        FlowNotification notification = FlowNotification.builder()
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .reference(flow.getReference())
                .instrumentType(flow.getInstrumentType() != null ? flow.getInstrumentType().name() : null)
                .phase(phase)
                .status(status)
                .traceOriginalId(flow.getTraceOriginalId())
                .signingStatus(flow.getSigningStatus())
                .transferId(flow.getTransferId())
                .approveUrl("/flows/enigio-instrument/" + flow.getId() + "/approve")
                .timestamp(Instant.now())
                .build();

        try {
            String json = objectMapper.writeValueAsString(notification);
            kafkaTemplate.send(notificationTopic, flow.getId(), json);
            log.info("[{}] Published {} notification (phase={}, status={})",
                    flow.getId(), phase, phase, status);
        } catch (Exception e) {
            log.error("[{}] Failed to publish notification: {}", flow.getId(), e.getMessage());
        }
    }
}
