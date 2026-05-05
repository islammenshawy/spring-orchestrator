package com.dis.instrument.service;

import com.dis.instrument.model.FlowStep;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.kafka.StepCommandMessage;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Expiry + safety-net scheduler for parked flows.
 *
 * Gate steps (AWAIT_*_APPROVAL, AWAIT_SIGNATURES) park flows in MongoDB and
 * exit Kafka entirely. Re-activation is event-driven:
 *   1. POST /approve (downstream) → publishes step command to Kafka
 *   2. Webhook (Enigio) → publishes step command to Kafka
 *   3. This scheduler (safety net) → re-publishes stale flows every 5 min
 *
 * The scheduler catches:
 *   - Missed webhooks (Enigio failed to deliver)
 *   - Missed approvals (downstream didn't call approve)
 *   - Expiry detection (step checks threshold on re-execution, fails if expired)
 */
@Slf4j
@Component
public class WaitingFlowScheduler {

    private static final List<String> WAIT_STEPS = java.util.Arrays.stream(FlowStep.values())
            .filter(FlowStep::isGate)
            .map(FlowStep::name)
            .toList();

    private final MongoTemplate mongoTemplate;
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String commandTopic;
    private final int pollIntervalMinutes;

    @SuppressWarnings("rawtypes")
    public WaitingFlowScheduler(MongoTemplate mongoTemplate,
                                KafkaTemplate kafkaTemplate,
                                ObjectMapper objectMapper,
                                @Value("${orchestrator.kafka.command-topic:dis.instrument.commands}") String commandTopic,
                                @Value("${dis.signing.poll-interval-minutes:30}") int pollIntervalMinutes) {
        this.mongoTemplate = mongoTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.commandTopic = commandTopic;
        this.pollIntervalMinutes = pollIntervalMinutes;
    }

    /**
     * Scan for parked flows and re-publish their current step.
     * Runs every 5 min (default). Acts as safety net for missed webhooks/approvals
     * and triggers expiry checks on re-execution.
     */
    @Scheduled(fixedDelayString = "${dis.expiry.poll-interval-ms:300000}")
    public void pollWaitingFlows() {
        // Find flows that are WAITING_RETRY at a wait step and haven't been updated recently
        Instant staleThreshold = Instant.now().minus(pollIntervalMinutes, ChronoUnit.MINUTES);

        Query query = Query.query(Criteria.where("status").is(FlowStatus.WAITING_RETRY.name())
                .and("currentStep").in(WAIT_STEPS)
                .and("updatedAt").lt(staleThreshold));

        List<EnigioInstrumentEntity> waitingFlows = mongoTemplate.find(
                query, EnigioInstrumentEntity.class);

        if (waitingFlows.isEmpty()) return;

        log.info("[WaitScheduler] Found {} flows in wait states, re-publishing", waitingFlows.size());

        for (EnigioInstrumentEntity flow : waitingFlows) {
            try {
                StepCommandMessage cmd = StepCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flow.getId())
                        .correlationId(flow.getCorrelationId())
                        .stepName(flow.getCurrentStep())
                        .flowType("enigio-instrument")
                        .build();

                String json = objectMapper.writeValueAsString(cmd);
                String partitionKey = flow.getCorrelationId() != null
                        ? flow.getCorrelationId() : flow.getId();
                kafkaTemplate.send(commandTopic, partitionKey, json).get();

                // Update timestamp to prevent re-publishing on next cycle
                flow.setUpdatedAt(Instant.now());
                mongoTemplate.save(flow);

                log.info("[WaitScheduler] Re-published {} for flow {} (waiting since {})",
                        flow.getCurrentStep(), flow.getId(),
                        Duration.between(
                                flow.getSigningStartedAt() != null ? flow.getSigningStartedAt() : flow.getUpdatedAt(),
                                Instant.now()).toMinutes() + "m ago");
            } catch (Exception e) {
                log.error("[WaitScheduler] Failed to re-publish flow {}: {}", flow.getId(), e.getMessage());
            }
        }
    }
}
