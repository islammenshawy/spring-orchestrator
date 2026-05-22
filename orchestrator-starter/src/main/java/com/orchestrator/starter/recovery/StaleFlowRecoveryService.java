package com.orchestrator.starter.recovery;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import com.orchestrator.starter.kafka.StepCommandMessage;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Recovers flows stuck in IN_PROGRESS after a container crash.
 * Re-publishes the current step command to Kafka.
 *
 * Uses atomic batch claiming via MongoDB updateMulti + claimedBy/claimedAt
 * to ensure each flow is processed by exactly one pod in a multi-pod deployment.
 *
 * Pattern: claim batch → find claimed → process → release
 *
 * Guards against false positives:
 * - Skips flows with pending outbox events (pipeline is just busy)
 * - Uses configurable stale threshold (default 15 min, must exceed retry budget)
 * - Caps recovery at maxRecoveryAttempts to prevent infinite recovery loops
 * - Orphan cleanup releases claims from crashed pods (claimTtl)
 */
@Slf4j
public class StaleFlowRecoveryService {

    private final FlowTypeRegistry registry;
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MongoTemplate mongoTemplate;
    private final int staleThresholdMinutes;
    private final int maxRecoveryAttempts;
    private final int batchSize;
    private final int claimTtlMinutes;
    private final String podId;
    private final com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository;
    private final OrchestratorMetrics metrics;

    public StaleFlowRecoveryService(FlowTypeRegistry registry, KafkaTemplate kafkaTemplate,
                                     ObjectMapper objectMapper, MongoTemplate mongoTemplate,
                                     int staleThresholdMinutes, int maxRecoveryAttempts,
                                     int batchSize, int claimTtlMinutes,
                                     com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository,
                                     OrchestratorMetrics metrics) {
        this.registry = registry;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.mongoTemplate = mongoTemplate;
        this.staleThresholdMinutes = staleThresholdMinutes;
        this.maxRecoveryAttempts = maxRecoveryAttempts;
        this.batchSize = batchSize;
        this.claimTtlMinutes = claimTtlMinutes;
        this.outboxRepository = outboxRepository;
        this.metrics = metrics != null ? metrics : OrchestratorMetrics.noop();

        // Pod identity for claim ownership — unique per JVM
        String hostname = System.getenv("HOSTNAME");
        this.podId = (hostname != null && !hostname.isBlank())
                ? hostname : "pod-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("[Recovery] Pod identity: {}, batchSize={}, claimTtl={}min",
                this.podId, batchSize, claimTtlMinutes);
    }

    @Scheduled(fixedDelayString = "${orchestrator.recovery.scan-interval-ms:30000}")
    @SuppressWarnings("unchecked")
    public void recoverStaleFlows() {
        for (FlowTypeDescriptor descriptor : registry.getAll()) {
            String commandTopic = descriptor.getCommandTopic();
            if (commandTopic == null || commandTopic.isBlank()) continue;
            if (descriptor.getEntityClass() == null || descriptor.getEntityClass() == Object.class) continue;

            // Release orphaned claims first (crashed pods)
            releaseOrphanedClaims(descriptor.getEntityClass());

            recoverFlowType(descriptor.getFlowType(), descriptor.getEntityClass(),
                    commandTopic, descriptor.getStepRegistry());

            expireWaitingFlows(descriptor.getFlowType(), descriptor.getEntityClass(),
                    descriptor.getStepRegistry());
        }
    }

    /**
     * Release claims from pods that crashed before finishing processing.
     * A claim is orphaned if claimedAt + claimTtl < now.
     */
    private void releaseOrphanedClaims(Class<?> entityClass) {
        Instant orphanThreshold = Instant.now().minus(claimTtlMinutes, ChronoUnit.MINUTES);

        long released = mongoTemplate.updateMulti(
                Query.query(Criteria.where("claimedBy").ne(null)
                        .and("claimedAt").lt(orphanThreshold)),
                new Update().set("claimedBy", null).set("claimedAt", null),
                entityClass).getModifiedCount();

        if (released > 0) {
            log.warn("[Recovery] Released {} orphaned claims (claimTtl={}min)", released, claimTtlMinutes);
        }
    }

    /**
     * Atomic batch claim → find → process → release pattern.
     *
     * 1. updateMulti: atomically set claimedBy=podId on up to batchSize unclaimed stale flows
     * 2. find: retrieve the claimed batch
     * 3. process: for each flow, check outbox + recovery count, then publish to Kafka
     * 4. release: clear claimedBy/claimedAt, bump updatedAt, inc recoveryCount
     */
    @SuppressWarnings("unchecked")
    private void recoverFlowType(String flowType, Class<?> entityClass,
                                  String commandTopic, StepRegistry<?> stepRegistry) {
        Instant threshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);

        // Step 1: Find candidate IDs (limited to batchSize)
        Query candidateQuery = Query.query(Criteria.where("status").is(FlowStatus.IN_PROGRESS.name())
                .and("updatedAt").lt(threshold)
                .and("claimedBy").is(null))
                .limit(batchSize);
        candidateQuery.fields().include("_id");
        List<?> candidates = mongoTemplate.find(candidateQuery, entityClass);
        if (candidates.isEmpty()) return;

        List<String> candidateIds = candidates.stream()
                .map(c -> ((OrchestratorFlow) c).getId())
                .toList();

        // Step 2: Claim those IDs atomically via updateMulti with $in
        long claimed = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(candidateIds)
                        .and("claimedBy").is(null)),
                new Update()
                        .set("claimedBy", podId)
                        .set("claimedAt", Instant.now()),
                entityClass).getModifiedCount();

        if (claimed == 0) return;

        // Step 3: Find the claimed batch
        List<?> batch = mongoTemplate.find(
                Query.query(Criteria.where("claimedBy").is(podId)
                        .and("status").is(FlowStatus.IN_PROGRESS.name())),
                entityClass);

        log.info("[Recovery] Claimed {} stale flows for type '{}' (pod: {})", batch.size(), flowType, podId);

        // Step 3: Process each flow
        for (Object obj : batch) {
            OrchestratorFlow flow = (OrchestratorFlow) obj;

            // Filter out flows with pending outbox events — pipeline is just busy
            if (outboxRepository != null && outboxRepository.countByFlowIdAndPublishedFalse(flow.getId()) > 0) {
                releaseClaim(flow.getId(), entityClass);
                continue;
            }

            // Recovery loop detection: cap at maxRecoveryAttempts
            if (flow.getRecoveryCount() >= maxRecoveryAttempts) {
                log.error("[Recovery] Flow {} exceeded max recovery attempts ({}) — marking FAILED",
                        flow.getId(), maxRecoveryAttempts);
                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .set("status", FlowStatus.FAILED.name())
                                .set("errorMessage", "Exceeded max recovery attempts (" + maxRecoveryAttempts + ")")
                                .set("updatedAt", Instant.now())
                                .set("claimedBy", null)
                                .set("claimedAt", null),
                        entityClass);
                metrics.flowFailed(flowType);
                continue;
            }

            try {
                StepCommandMessage cmd = StepCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flow.getId())
                        .correlationId(flow.getCorrelationId())
                        .stepName(flow.getCurrentStep())
                        .flowType(flowType)
                        .build();
                String partitionKey = flow.getCorrelationId() != null
                        ? flow.getCorrelationId() : flow.getId();
                kafkaTemplate.send(commandTopic, partitionKey,
                        objectMapper.writeValueAsString(cmd)).get();

                // Step 4: Release claim + increment recoveryCount atomically
                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .inc("recoveryCount", 1)
                                .set("updatedAt", Instant.now())
                                .set("claimedBy", null)
                                .set("claimedAt", null),
                        entityClass);
                metrics.recoveryRecovered(flowType);
                log.info("[Recovery] Re-published step {} for flow {} (type: {}, attempt: {})",
                        flow.getCurrentStep(), flow.getId(), flowType, flow.getRecoveryCount() + 1);
            } catch (Exception e) {
                // Release claim on failure so another pod or next cycle can retry
                releaseClaim(flow.getId(), entityClass);
                log.error("[Recovery] Failed to recover flow {} (type: {}): {}",
                        flow.getId(), flowType, e.getMessage());
            }
        }
    }

    /**
     * Expire WAITING_RETRY flows that exceeded step-level expiresAfter.
     * Uses batch claiming to prevent duplicate expiry across pods.
     */
    @SuppressWarnings("unchecked")
    private void expireWaitingFlows(String flowType, Class<?> entityClass,
                                     StepRegistry<?> stepRegistry) {
        if (stepRegistry == null) return;

        // Find all step handlers with expiresAfter configured
        for (String stepName : stepRegistry.getStepNames()) {
            StepHandler<?> handler = stepRegistry.getHandler(stepName);
            if (handler == null) continue;

            java.time.Duration expiresAfter = handler.getExpiresAfter();
            if (expiresAfter == null) continue;

            Instant expiryThreshold = Instant.now().minus(expiresAfter);

            // Find candidate IDs (limited to batchSize)
            Query expiryCandidateQuery = Query.query(Criteria.where("status").is(FlowStatus.WAITING_RETRY.name())
                    .and("currentStep").is(stepName)
                    .and("waitingSince").lt(expiryThreshold)
                    .and("claimedBy").is(null))
                    .limit(batchSize);
            expiryCandidateQuery.fields().include("_id");
            List<?> expiryCandidates = mongoTemplate.find(expiryCandidateQuery, entityClass);
            if (expiryCandidates.isEmpty()) continue;

            List<String> expiryIds = expiryCandidates.stream()
                    .map(c -> ((OrchestratorFlow) c).getId())
                    .toList();

            // Claim those IDs atomically
            long claimed = mongoTemplate.updateMulti(
                    Query.query(Criteria.where("_id").in(expiryIds)
                            .and("claimedBy").is(null)),
                    new Update()
                            .set("claimedBy", podId)
                            .set("claimedAt", Instant.now()),
                    entityClass).getModifiedCount();

            if (claimed == 0) continue;

            List<?> batch = mongoTemplate.find(
                    Query.query(Criteria.where("claimedBy").is(podId)
                            .and("status").is(FlowStatus.WAITING_RETRY.name())
                            .and("currentStep").is(stepName)),
                    entityClass);

            for (Object obj : batch) {
                OrchestratorFlow flow = (OrchestratorFlow) obj;
                Instant waitingSince = flow.getWaitingSince();
                if (waitingSince == null) {
                    releaseClaim(flow.getId(), entityClass);
                    continue;
                }

                long waitedHours = java.time.Duration.between(waitingSince, Instant.now()).toHours();
                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .set("status", FlowStatus.FAILED.name())
                                .set("errorMessage", "Step " + stepName + " expired after " + waitedHours +
                                        "h (limit: " + expiresAfter.toHours() + "h)")
                                .set("updatedAt", Instant.now())
                                .set("claimedBy", null)
                                .set("claimedAt", null),
                        entityClass);
                metrics.flowFailed(flowType);
                log.info("[Recovery] Expired flow {} at step {} (waited {}h, limit {}h)",
                        flow.getId(), stepName, waitedHours, expiresAfter.toHours());
            }
        }
    }

    /** Release a claim without modifying any other fields. */
    private void releaseClaim(String flowId, Class<?> entityClass) {
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update().set("claimedBy", null).set("claimedAt", null),
                entityClass);
    }
}
