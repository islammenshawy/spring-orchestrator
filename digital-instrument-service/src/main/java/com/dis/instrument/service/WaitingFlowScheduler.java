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
import org.springframework.data.mongodb.core.query.Update;
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
 * Uses atomic batch claiming via MongoDB updateMulti + claimedBy/claimedAt
 * to ensure each flow is processed by exactly one pod in a multi-pod deployment.
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
    private final int batchSize;
    private final int claimTtlMinutes;
    private final String podId;

    @SuppressWarnings("rawtypes")
    public WaitingFlowScheduler(MongoTemplate mongoTemplate,
                                KafkaTemplate kafkaTemplate,
                                ObjectMapper objectMapper,
                                @Value("${orchestrator.kafka.command-topic:dis.instrument.commands}") String commandTopic,
                                @Value("${dis.signing.poll-interval-minutes:30}") int pollIntervalMinutes,
                                @Value("${orchestrator.recovery.batch-size:100}") int batchSize,
                                @Value("${orchestrator.recovery.claim-ttl-minutes:5}") int claimTtlMinutes) {
        this.mongoTemplate = mongoTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.commandTopic = commandTopic;
        this.pollIntervalMinutes = pollIntervalMinutes;
        this.batchSize = batchSize;
        this.claimTtlMinutes = claimTtlMinutes;

        String hostname = System.getenv("HOSTNAME");
        this.podId = (hostname != null && !hostname.isBlank())
                ? hostname : "pod-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("[WaitScheduler] Pod identity: {}, batchSize={}, claimTtl={}min",
                this.podId, batchSize, claimTtlMinutes);
    }

    /**
     * Scan for parked flows and re-publish their current step.
     * Runs every 5 min (default). Acts as safety net for missed webhooks/approvals
     * and triggers expiry checks on re-execution.
     *
     * Uses batch claiming: updateMulti → find → process → release.
     */
    @Scheduled(fixedDelayString = "${dis.expiry.poll-interval-ms:300000}")
    public void pollWaitingFlows() {
        // Release orphaned claims from crashed pods
        releaseOrphanedClaims();

        Instant staleThreshold = Instant.now().minus(pollIntervalMinutes, ChronoUnit.MINUTES);

        // Step 1: Find candidate IDs (limited to batchSize)
        Query candidateQuery = Query.query(Criteria.where("status").is(FlowStatus.WAITING_RETRY.name())
                .and("currentStep").in(WAIT_STEPS)
                .and("updatedAt").lt(staleThreshold)
                .and("claimedBy").is(null))
                .limit(batchSize);
        candidateQuery.fields().include("_id");
        List<EnigioInstrumentEntity> candidates = mongoTemplate.find(candidateQuery, EnigioInstrumentEntity.class);
        if (candidates.isEmpty()) return;

        List<String> candidateIds = candidates.stream()
                .map(EnigioInstrumentEntity::getId)
                .toList();

        // Step 2: Claim those IDs atomically via updateMulti with $in
        long claimed = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(candidateIds)
                        .and("claimedBy").is(null)),
                new Update()
                        .set("claimedBy", podId)
                        .set("claimedAt", Instant.now()),
                EnigioInstrumentEntity.class).getModifiedCount();

        if (claimed == 0) return;

        // Step 3: Find the claimed batch
        List<EnigioInstrumentEntity> batch = mongoTemplate.find(
                Query.query(Criteria.where("claimedBy").is(podId)
                        .and("status").is(FlowStatus.WAITING_RETRY.name())
                        .and("currentStep").in(WAIT_STEPS)),
                EnigioInstrumentEntity.class);

        log.info("[WaitScheduler] Claimed {} flows in wait states (pod: {})", batch.size(), podId);

        // Step 3: Process each flow
        for (EnigioInstrumentEntity flow : batch) {
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

                // Step 4: Release claim + bump updatedAt
                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .set("updatedAt", Instant.now())
                                .set("claimedBy", null)
                                .set("claimedAt", null),
                        EnigioInstrumentEntity.class);

                log.info("[WaitScheduler] Re-published {} for flow {} (waiting since {})",
                        flow.getCurrentStep(), flow.getId(),
                        Duration.between(
                                flow.getSigningStartedAt() != null ? flow.getSigningStartedAt() : flow.getUpdatedAt(),
                                Instant.now()).toMinutes() + "m ago");
            } catch (Exception e) {
                // Release claim on failure so next cycle can retry
                releaseClaim(flow.getId());
                log.error("[WaitScheduler] Failed to re-publish flow {}: {}", flow.getId(), e.getMessage());
            }
        }
    }

    private void releaseOrphanedClaims() {
        Instant orphanThreshold = Instant.now().minus(claimTtlMinutes, ChronoUnit.MINUTES);

        long released = mongoTemplate.updateMulti(
                Query.query(Criteria.where("claimedBy").ne(null)
                        .and("claimedAt").lt(orphanThreshold)
                        .and("status").is(FlowStatus.WAITING_RETRY.name())),
                new Update().set("claimedBy", null).set("claimedAt", null),
                EnigioInstrumentEntity.class).getModifiedCount();

        if (released > 0) {
            log.warn("[WaitScheduler] Released {} orphaned claims (claimTtl={}min)", released, claimTtlMinutes);
        }
    }

    private void releaseClaim(String flowId) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update().set("claimedBy", null).set("claimedAt", null),
                EnigioInstrumentEntity.class);
    }
}
