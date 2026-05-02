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
 * Downstream systems consume these to track progress, approve next phases,
 * upload additional documents, or reconcile with the vendor.
 *
 * Every notification carries all actionable URLs so downstream never needs
 * to construct API paths — just follow the links in the payload.
 */
@Slf4j
@Component
public class FlowNotificationPublisher {

    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String notificationTopic;
    private final String baseUrl;

    @SuppressWarnings("rawtypes")
    public FlowNotificationPublisher(KafkaTemplate kafkaTemplate,
                                     ObjectMapper objectMapper,
                                     @Value("${dis.notifications.topic:dis.instrument.notifications}") String topic,
                                     @Value("${dis.base-url:http://digital-instrument-service:8087}") String baseUrl) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.notificationTopic = topic;
        this.baseUrl = baseUrl;
    }

    @SuppressWarnings("unchecked")
    public void notifyPhaseComplete(EnigioInstrumentEntity flow, String phase, String status) {
        // Dedup: skip if same phase+status was already published for this flow
        if (phase.equals(flow.getLastNotifiedPhase()) && status.equals(flow.getLastNotifiedStatus())) {
            log.debug("[{}] Notification dedup: phase={}, status={} already published — skipping",
                    flow.getId(), phase, status);
            return;
        }

        String flowPath = "/flows/enigio-instrument/" + flow.getId();

        FlowNotification notification = FlowNotification.builder()
                // Identity
                .instrumentId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .reference(flow.getReference())
                .instrumentType(flow.getInstrumentType() != null ? flow.getInstrumentType().name() : null)
                // Phase
                .phase(phase)
                .status(status)
                .currentStep(flow.getCurrentStep())
                // Vendor state
                .traceOriginalId(flow.getTraceOriginalId())
                .signingStatus(flow.getSigningStatus())
                .transferId(flow.getTransferId())
                // Actionable URLs
                .approveUrl(baseUrl + flowPath + "/approve")
                .cancelUrl(baseUrl + flowPath + "/cancel")
                .statusUrl(baseUrl + flowPath)
                .approvalStatusUrl(baseUrl + flowPath + "/approval-status")
                .vendorSyncUrl(flow.getTraceOriginalId() != null
                        ? baseUrl + "/vendor/enigio/documents/" + flow.getTraceOriginalId()
                        : null)
                .additionalDocumentsUrl(baseUrl + "/documents/additional")
                .timestamp(Instant.now())
                .build();

        try {
            String json = objectMapper.writeValueAsString(notification);
            kafkaTemplate.send(notificationTopic, flow.getId(), json);
            flow.setLastNotifiedPhase(phase);
            flow.setLastNotifiedStatus(status);
            log.info("[{}] Published notification: phase={}, status={}",
                    flow.getId(), phase, status);
        } catch (Exception e) {
            log.error("[{}] Failed to publish notification: {}", flow.getId(), e.getMessage());
        }
    }
}
