package com.dis.instrument.core.api;

import com.dis.instrument.vendor.enigio.EnigioInstrumentEntity;
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
 * Safety-net scheduler for flows in wait states (webhook gates + approval gates).
 *
 * Flow architecture for wait steps:
 *   1. Webhook (primary) — real-time, sets the flag directly in MongoDB
 *   2. Kafka retry (short-term) — catches webhook if it arrives within retry window
 *   3. This scheduler (long-term) — re-publishes flows that exhausted Kafka retries
 *      but the signal (webhook/approval) hasn't arrived yet
 *
 * The step's completedWhen guard prevents duplicate work — if the webhook
 * already set the flag, the re-published command just advances the flow.
 *
 * Expiry is checked by the step itself on each execution (Kafka retry or scheduler).
 * When elapsed > threshold, the step throws NonRetryableStepException → FAILED.
 */
@Slf4j
@Component
public class WaitingFlowScheduler {

    private static final List<String> WAIT_STEPS = List.of(
            "AWAIT_PREPARATION_APPROVAL",
            "AWAIT_SIGNATURES",
            "AWAIT_DELIVERY_APPROVAL"
    );

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
     * Scan for flows stuck in wait states and re-publish their current step.
     * Runs at dis.signing.poll-interval-minutes (default 30min).
     */
    @Scheduled(fixedDelayString = "${dis.signing.poll-interval-ms:1800000}")
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
                kafkaTemplate.send(commandTopic, flow.getId(), json).get();

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
