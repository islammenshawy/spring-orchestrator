package com.orchestrator.starter.recovery;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
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
 * Recovers and re-delivers flows across all waiting states:
 *
 * 1. IN_PROGRESS (stale) — container crash recovery. Re-publishes current step.
 * 2. WAITING_RETRY (polling) — re-delivers when nextRetryAt elapses. Used by pollUntil().
 * 3. PARKED (safety net) — re-delivers gate steps not touched in a long time (missed webhooks).
 * 4. Expiry — fails flows past their expiresAt deadline (set by waitUntil/pollUntil).
 *
 * Uses atomic batch claiming via MongoDB updateMulti + claimedBy/claimedAt
 * to ensure each flow is processed by exactly one pod in a multi-pod deployment.
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

            redeliverPollingFlows(descriptor.getFlowType(), descriptor.getEntityClass(), commandTopic);

            redeliverParkedSafetyNet(descriptor.getFlowType(), descriptor.getEntityClass(), commandTopic);

            expireWaitingFlows(descriptor.getFlowType(), descriptor.getEntityClass());

            recoverStuckCompensation(descriptor);
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

        // Step 2: Claim those IDs atomically — include status check to prevent claim hang
        // If status changed between find and claim, the flow won't be claimed
        long claimed = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(candidateIds)
                        .and("status").is(FlowStatus.IN_PROGRESS.name())
                        .and("claimedBy").is(null)),
                new Update()
                        .set("claimedBy", podId)
                        .set("claimedAt", Instant.now()),
                entityClass).getModifiedCount();

        if (claimed == 0) return;

        // Step 3: Find the claimed batch — re-verify status is still IN_PROGRESS
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
     * Re-deliver flows whose nextRetryAt has elapsed:
     * - WAITING_RETRY: polling steps (pollUntil)
     * - PARKED with nextRetryAt: sleeping steps (sleep/sleepUntil)
     */
    @SuppressWarnings("unchecked")
    private void redeliverPollingFlows(String flowType, Class<?> entityClass, String commandTopic) {
        Instant now = Instant.now();

        Query candidateQuery = Query.query(new Criteria().andOperator(
                Criteria.where("nextRetryAt").lt(now),
                Criteria.where("claimedBy").is(null),
                new Criteria().orOperator(
                        Criteria.where("status").is(FlowStatus.WAITING_RETRY.name()),
                        Criteria.where("status").is(FlowStatus.PARKED.name())
                )))
                .limit(batchSize);
        candidateQuery.fields().include("_id", "status");
        List<?> candidates = mongoTemplate.find(candidateQuery, entityClass);
        if (candidates.isEmpty()) return;

        republishBatch(candidates, entityClass, commandTopic, flowType,
                null, "polling/sleeping");
    }

    /**
     * Safety net for PARKED gate steps — re-delivers flows not touched in a long time.
     * Catches missed webhooks or failed API re-activation calls.
     * Uses staleThresholdMinutes as the staleness window.
     */
    @SuppressWarnings("unchecked")
    private void redeliverParkedSafetyNet(String flowType, Class<?> entityClass, String commandTopic) {
        Instant staleThreshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);

        Query candidateQuery = Query.query(Criteria.where("status").is(FlowStatus.PARKED.name())
                .and("updatedAt").lt(staleThreshold)
                .and("claimedBy").is(null))
                .limit(batchSize);
        candidateQuery.fields().include("_id");
        List<?> candidates = mongoTemplate.find(candidateQuery, entityClass);
        if (candidates.isEmpty()) return;

        republishBatch(candidates, entityClass, commandTopic, flowType,
                FlowStatus.PARKED, "parked-safety-net");
    }

    /** Claim a batch of candidate flows, re-publish to Kafka, release claims. */
    @SuppressWarnings("unchecked")
    private void republishBatch(List<?> candidates, Class<?> entityClass,
                                 String commandTopic, String flowType,
                                 FlowStatus expectedStatus, String label) {
        List<String> candidateIds = candidates.stream()
                .map(c -> ((OrchestratorFlow) c).getId())
                .toList();

        // Include status check in claim to prevent claiming flows that changed status
        // between candidate query and claim (e.g., completed by another pod)
        var claimCriteria = Criteria.where("_id").in(candidateIds)
                .and("claimedBy").is(null);
        if (expectedStatus != null) {
            claimCriteria = claimCriteria.and("status").is(expectedStatus.name());
        }
        long claimed = mongoTemplate.updateMulti(
                Query.query(claimCriteria),
                new Update()
                        .set("claimedBy", podId)
                        .set("claimedAt", Instant.now()),
                entityClass).getModifiedCount();

        if (claimed == 0) return;

        var claimedCriteria = Criteria.where("claimedBy").is(podId);
        List<?> batch = mongoTemplate.find(Query.query(claimedCriteria), entityClass);

        log.info("[Recovery] Claimed {} {} flows for type '{}' (pod: {})",
                batch.size(), label, flowType, podId);

        for (Object obj : batch) {
            OrchestratorFlow flow = (OrchestratorFlow) obj;
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

                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .set("updatedAt", Instant.now())
                                .set("claimedBy", null)
                                .set("claimedAt", null),
                        entityClass);
                log.info("[Recovery] Re-published {} for flow {} [{}]",
                        flow.getCurrentStep(), flow.getId(), label);
            } catch (Exception e) {
                releaseClaim(flow.getId(), entityClass);
                log.error("[Recovery] Failed to re-publish flow {} [{}]: {}",
                        flow.getId(), label, e.getMessage());
            }
        }
    }

    /**
     * Expire PARKED/WAITING_RETRY flows past their expiresAt deadline.
     * The deadline is set by waitUntil()/pollUntil() on first park.
     * Uses batch claiming to prevent duplicate expiry across pods.
     */
    @SuppressWarnings("unchecked")
    private void expireWaitingFlows(String flowType, Class<?> entityClass) {
        Instant now = Instant.now();

        // Single query: any flow with expiresAt in the past
        Query expiryCandidateQuery = Query.query(Criteria.where("status")
                .in(FlowStatus.WAITING_RETRY.name(), FlowStatus.PARKED.name())
                .and("expiresAt").lt(now)
                .and("claimedBy").is(null))
                .limit(batchSize);
        expiryCandidateQuery.fields().include("_id");
        List<?> expiryCandidates = mongoTemplate.find(expiryCandidateQuery, entityClass);
        if (expiryCandidates.isEmpty()) return;

        List<String> expiryIds = expiryCandidates.stream()
                .map(c -> ((OrchestratorFlow) c).getId())
                .toList();

        long claimed = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(expiryIds)
                        .and("claimedBy").is(null)
                        .and("status").in(FlowStatus.WAITING_RETRY.name(), FlowStatus.PARKED.name())),
                new Update()
                        .set("claimedBy", podId)
                        .set("claimedAt", now),
                entityClass).getModifiedCount();

        if (claimed == 0) return;

        List<?> batch = mongoTemplate.find(
                Query.query(Criteria.where("claimedBy").is(podId)
                        .and("status").in(FlowStatus.WAITING_RETRY.name(), FlowStatus.PARKED.name())
                        .and("expiresAt").lt(now)),
                entityClass);

        for (Object obj : batch) {
            OrchestratorFlow flow = (OrchestratorFlow) obj;
            long waitedHours = flow.getWaitingSince() != null
                    ? java.time.Duration.between(flow.getWaitingSince(), now).toHours() : 0;
            mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(flow.getId())),
                    new Update()
                            .set("status", FlowStatus.FAILED.name())
                            .set("errorMessage", "Step " + flow.getCurrentStep() +
                                    " expired after " + waitedHours + "h")
                            .set("updatedAt", now)
                            .set("claimedBy", null)
                            .set("claimedAt", null),
                    entityClass);
            metrics.flowFailed(flowType);
            log.info("[Recovery] Expired flow {} at step {} (waited {}h)",
                    flow.getId(), flow.getCurrentStep(), waitedHours);
        }
    }

    /**
     * Recover flows stuck in COMPENSATING or CANCELLING status after a crash.
     * These intermediate states have no auto-recovery — the recovery scanner
     * must detect and re-run compensation/cancellation.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void recoverStuckCompensation(FlowTypeDescriptor descriptor) {
        Instant threshold = Instant.now().minus(staleThresholdMinutes, ChronoUnit.MINUTES);
        Class<?> entityClass = descriptor.getEntityClass();

        Query candidateQuery = Query.query(Criteria.where("status")
                .in(FlowStatus.COMPENSATING.name(), FlowStatus.CANCELLING.name())
                .and("updatedAt").lt(threshold)
                .and("claimedBy").is(null))
                .limit(batchSize);
        candidateQuery.fields().include("_id", "status");
        List<?> candidates = mongoTemplate.find(candidateQuery, entityClass);
        if (candidates.isEmpty()) return;

        List<String> ids = candidates.stream()
                .map(c -> ((OrchestratorFlow) c).getId()).toList();

        long claimed = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(ids)
                        .and("claimedBy").is(null)
                        .and("status").in(FlowStatus.COMPENSATING.name(), FlowStatus.CANCELLING.name())),
                new Update().set("claimedBy", podId).set("claimedAt", Instant.now()),
                entityClass).getModifiedCount();
        if (claimed == 0) return;

        List<?> batch = mongoTemplate.find(
                Query.query(Criteria.where("claimedBy").is(podId)
                        .and("status").in(FlowStatus.COMPENSATING.name(), FlowStatus.CANCELLING.name())),
                entityClass);

        log.info("[Recovery] Found {} stuck compensation/cancellation flows (pod: {})", batch.size(), podId);

        for (Object obj : batch) {
            OrchestratorFlow flow = (OrchestratorFlow) obj;
            try {
                if (FlowStatus.COMPENSATING.name().equals(flow.getStatus().name())) {
                    log.info("[Recovery] Re-running compensation for stuck flow {}", flow.getId());
                    descriptor.getOrchestrator().retryCompensation(flow.getId());
                } else {
                    log.info("[Recovery] Re-running cancellation for stuck flow {}", flow.getId());
                    descriptor.getOrchestrator().cancelFlow(flow.getId(), "recovery: stuck in CANCELLING");
                }
            } catch (Exception e) {
                log.error("[Recovery] Failed to recover stuck flow {}: {}", flow.getId(), e.getMessage());
            } finally {
                releaseClaim(flow.getId(), entityClass);
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
