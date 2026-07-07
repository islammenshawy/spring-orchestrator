package com.orchestrator.starter.flow;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.StepOutcome;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.kafka.StepCommandMessage;
import com.orchestrator.starter.kafka.StepReplyMessage;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Saga orchestrator with transactional outbox.
 *
 * Executes steps one-per-Kafka-message, persists state + outbox atomically,
 * logs every step attempt, and runs compensation in reverse on permanent failure.
 */
@Slf4j
public class FlowOrchestrator<F extends OrchestratorFlow> {

    public static final String DEFAULT_FLOW_TYPE = "default";

    private final OrchestratorFlowRepository<F> flowRepository;
    private final StepRegistry<F> stepRegistry;
    private final OutboxEventRepository outboxRepository;
    private final StepExecutionLogRepository stepLogRepository;
    private final ObjectMapper objectMapper;
    private final String flowType;
    private final String commandTopic;
    private final String replyTopic;
    private final boolean replyEnabled;
    private final TransactionTemplate txTemplate;
    private final boolean includeFlowStateInLogs;
    private final KafkaTemplate kafkaTemplate;
    private final int stepTimeoutSeconds;
    private final ExecutorService stepExecutor;
    private final OrchestratorMetrics metrics;
    @Setter private int maxLogSnapshotBytes = 32_768; // 32 KB default
    @Setter private Class<F> entityClass;
    @Setter private org.springframework.data.mongodb.core.MongoTemplate mongoTemplate;
    @Setter private jakarta.validation.Validator validator;
    /** DC identifier for cross-DC log correlation. Set from kafka.cluster-id config. */
    @Setter private String dcId;
    /** Pod/instance ID for step execution claim. Prevents concurrent step execution on rebalance. */
    @Setter private String podId;
    /** Retry backoff config (orchestrator.retry.*) applied to the WAITING_RETRY retry path so it is
     *  jittered like the non-blocking retry topics. Defaults mirror OrchestratorProperties.RetryConfig. */
    @Setter private long retryInitialIntervalMs = 3000;
    @Setter private double retryMultiplier = 2.0;
    @Setter private long retryMaxIntervalMs = 30000;
    @Setter private double retryJitterFactor = 0.5;

    @Builder
    public FlowOrchestrator(
            OrchestratorFlowRepository<F> flowRepository,
            StepRegistry<F> stepRegistry,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper,
            String flowType,
            String commandTopic,
            String replyTopic,
            boolean replyEnabled,
            TransactionTemplate txTemplate,
            boolean includeFlowStateInLogs,
            KafkaTemplate kafkaTemplate,
            int stepTimeoutSeconds,
            OrchestratorMetrics metrics) {
        this.flowRepository = flowRepository;
        this.stepRegistry = stepRegistry;
        this.outboxRepository = outboxRepository;
        this.stepLogRepository = stepLogRepository;
        this.objectMapper = objectMapper;
        this.flowType = flowType;
        this.commandTopic = commandTopic;
        this.replyTopic = replyTopic;
        this.replyEnabled = replyEnabled;
        this.includeFlowStateInLogs = includeFlowStateInLogs;
        this.txTemplate = txTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.stepTimeoutSeconds = stepTimeoutSeconds;
        this.stepExecutor = stepTimeoutSeconds > 0
                ? Executors.newVirtualThreadPerTaskExecutor() : null;
        this.metrics = metrics != null ? metrics : OrchestratorMetrics.noop();
    }

    /**
     * Starts a flow: saves to MongoDB, publishes first step to Kafka,
     * then returns. The caller gets the flow ID immediately.
     *
     * The Kafka publish happens synchronously (waits for broker ack)
     * so the message is guaranteed to be in Kafka before we return.
     * The outbox event is also written as a safety net for subsequent steps.
     */
    public F startFlow(F flow) {
        // Validate entity before starting (catches missing required fields early)
        if (validator != null) {
            var violations = validator.validate(flow);
            if (!violations.isEmpty()) {
                String errors = violations.stream()
                        .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                        .collect(java.util.stream.Collectors.joining(", "));
                throw new IllegalArgumentException("Invalid flow entity: " + errors);
            }
        }

        // Duplicate flow detection: if correlationId is set, check for existing flow
        if (flow.getCorrelationId() != null && mongoTemplate != null && entityClass != null) {
            var existing = mongoTemplate.findOne(
                    Query.query(
                            Criteria.where("correlationId").is(flow.getCorrelationId())
                                    .and("flowType").is(flowType)),
                    entityClass);
            if (existing != null) {
                log.info("[Saga] Duplicate flow detected — correlationId={} already exists as {}",
                        flow.getCorrelationId(), existing.getId());
                return existing;
            }
        }

        flow.setCurrentStep(stepRegistry.getFirstStep());
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setUpdatedAt(Instant.now());
        if (flowType != null) flow.setFlowType(flowType);

        // Save flow + outbox event atomically
        F savedFlow = runInTransaction(() -> {
            F f = flowRepository.save(flow);
            writeOutboxEvent(f);
            return f;
        }, flow);

        metrics.flowStarted(flowType);
        return savedFlow;
    }

    /**
     * Execute a step by name. For sequential steps, stepName matches currentStep.
     * For parallel steps, stepName is the specific parallel step from the Kafka message.
     *
     * Any infrastructure exception (MongoDB down, connection error) is wrapped as
     * RetryableStepException so Spring Kafka routes to retry topics rather than DLT.
     * Only explicit NonRetryableStepException (business errors like HTTP 400) goes to DLT.
     */
    public void executeStep(String flowId, String stepName) {
        try {
            doExecuteStep(flowId, stepName);
        } catch (RetryableStepException | NonRetryableStepException e) {
            throw e; // Already typed — let Spring Kafka handle
        } catch (Exception e) {
            // Infrastructure failure (MongoDB, network, etc.) — retryable
            log.warn("[Saga] Infrastructure error in step {} for flow {}: {}",
                    stepName, flowId, e.getMessage());
            throw new RetryableStepException(
                    "Infrastructure error in " + stepName + ": " + e.getMessage(), e);
        }
    }

    private void doExecuteStep(String flowId, String stepName) {
        MDC.put("flowId", flowId);
        MDC.put("flowType", flowType != null ? flowType : DEFAULT_FLOW_TYPE);
        if (stepName != null) MDC.put("stepName", stepName);
        // DC identifier for cross-DC log correlation
        if (dcId != null) MDC.put("dcId", dcId);
        try {
        doExecuteStepInner(flowId, stepName);
        } finally {
            MDC.remove("flowId");
            MDC.remove("flowType");
            MDC.remove("stepName");
            MDC.remove("dcId");
            MDC.remove("parentFlowId");
            MDC.remove("parentFlowType");
        }
    }

    private void doExecuteStepInner(String flowId, String stepName) {
        F flow = flowRepository.findById(flowId)
                .orElseThrow(() -> new NonRetryableStepException("Flow not found: " + flowId));

        // Add parent context to MDC for Splunk tracing
        if (flow.getParentFlowId() != null) {
            MDC.put("parentFlowId", flow.getParentFlowId());
            MDC.put("parentFlowType", flow.getParentFlowType());
        }

        if (flow.getStatus() == FlowStatus.COMPLETED) return;
        if (flow.getStatus() == FlowStatus.CANCELLED || flow.getStatus() == FlowStatus.CANCELLING) {
            log.info("[Saga] Flow {} is {} — skipping step {}", flowId, flow.getStatus(), stepName);
            return;
        }
        // Guard against crash-recovery re-delivery: if the flow was already moved to
        // WAITING_RETRY with a future nextRetryAt (crash after saving retry status but before
        // throwing), skip immediate re-execution — the scheduler will handle it at backoff time.
        // Only skip if nextRetryAt is in the future — past nextRetryAt means scheduler triggered this.
        if (flow.getStatus() == FlowStatus.WAITING_RETRY
                && flow.getNextRetryAt() != null && flow.getNextRetryAt().isAfter(Instant.now())) {
            log.debug("[Saga] Flow {} is WAITING_RETRY with future nextRetryAt — skipping, scheduler will handle", flowId);
            return;
        }

        // Use the step name from the Kafka message (supports parallel steps)
        if (stepName == null) stepName = flow.getCurrentStep();
        StepHandler<F> handler = stepRegistry.getHandler(stepName);

        // Check if this is a join point — all parallel steps must be done first
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isJoinPoint()) {
            String group = adapter.getJoinOnGroup();
            List<StepHandler<F>> parallelSteps = stepRegistry.getParallelGroup(group);
            Set<String> completed = flow.getCompletedSteps();
            boolean allDone = parallelSteps.stream()
                    .allMatch(ps -> completed.contains(ps.getStepName()));
            if (!allDone) {
                log.info("[Saga] Join {} waiting — not all parallel steps in group '{}' completed",
                        stepName, group);
                return; // Don't ack — message will be redelivered
            }
            log.info("[Saga] Join {} — all parallel steps in group '{}' completed, proceeding",
                    stepName, group);
        }

        // Layer 2 idempotency — skip if flow already advanced past this step
        if (flow.getCompletedSteps().contains(stepName)) {
            String currentStep = flow.getCurrentStep();
            if (currentStep != null && !currentStep.equals(stepName)) {
                log.debug("[Saga] Step {} completed, flow at {} — skipping", stepName, currentStep);
                return;
            }
            // Step completed but flow still here (gate re-activation or duplicate).
            // Fall through to execute — step handlers are idempotent.
        }

        // Atomic claim: prevent concurrent consumers from executing the same step.
        // On Kafka rebalance, two consumers may receive the same message. The first to
        // claim wins; the second sees modifiedCount=0 and skips.
        // Gate re-activation (step in completedSteps but currentStep matches) skips the
        // claim check — the step handler is idempotent for these cases.
        if (mongoTemplate != null && entityClass != null
                && !flow.getCompletedSteps().contains(stepName)) {
            long claimed = mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(flowId)
                            .and("executingStep").is(null)
                            .and("completedSteps").nin(stepName)),
                    new Update()
                            .set("executingStep", stepName)
                            .set("executingPod", podId)
                            .set("status", FlowStatus.IN_PROGRESS.name())
                            .set("updatedAt", Instant.now())
                            .inc("version", 1),
                    entityClass).getModifiedCount();
            if (claimed == 0) {
                log.info("[Saga] Step {} already claimed on flow {} — skipping (rebalance duplicate)",
                        stepName, flowId);
                return;
            }
            // Re-read to get updated version after claim
            flow = flowRepository.findById(flowId)
                    .orElseThrow(() -> new NonRetryableStepException("Flow not found after claim: " + flowId));
        } else {
            flow.setStatus(FlowStatus.IN_PROGRESS);
        }

        String flowBefore = includeFlowStateInLogs ? serializeForLog(flow) : null;
        Instant startedAt = Instant.now();

        log.info("[Saga] Executing step {} for flow {} (pod: {})", stepName, flowId, podId);

        try {
            executeWithTimeout(handler, flow, stepName);
        } catch (WaitingStepException e) {
            StepOutcome outcome = (e.isParked() || e.getWaitMode() == WaitingStepException.WaitMode.SLEEPING)
                    ? StepOutcome.PARKED : StepOutcome.WAITING;
            metrics.stepExecution(flowType, stepName, outcome.name(),
                    Duration.between(startedAt, Instant.now()));
            logStep(flowId, stepName, outcome.name(), flow.getRetryCount(),
                    flowBefore, null, e.getMessage(), startedAt);
            handleWaitingStep(flow, e);
            return;
        } catch (RetryableStepException e) {
            metrics.stepExecution(flowType, stepName, StepOutcome.RETRYING.name(),
                    Duration.between(startedAt, Instant.now()));
            logStep(flowId, stepName, StepOutcome.RETRYING.name(), flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handleRetryableFailure(flow, e);
            throw e;
        } catch (NonRetryableStepException e) {
            metrics.stepExecution(flowType, stepName, StepOutcome.FAILED.name(),
                    Duration.between(startedAt, Instant.now()));
            logStep(flowId, stepName, StepOutcome.FAILED.name(), flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handlePermanentFailure(flow, e);
            return;
        } catch (Exception e) {
            boolean recovered = handleUnexpectedStepError(handler, flow, flowId, stepName, flowBefore, startedAt, e);
            if (!recovered) return; // permanent failure or non-recoverable — don't advance
        }

        // Step succeeded — drain signals first (prevents $pushed signals from being
        // overwritten by saveFlowWithRetry), then mark completed and persist.
        drainPendingSignals(flow);

        // Complete step: mark completed + persist entire flow in one call.
        // flowRepository.save() is atomic at MongoDB level (single document replace).
        // Domain fields + completedSteps + reset fields all saved together.
        completeStep(flow, flowId, stepName);

        metrics.stepExecution(flowType, stepName, StepOutcome.COMPLETED.name(),
                Duration.between(startedAt, Instant.now()));

        if (replyEnabled) {
            publishReply(flowId, stepName, StepOutcome.COMPLETED.name(), null, serialize(flow));
        } else {
            markParallelStepCompleted(flow, stepName, handler);
        }

        logStep(flowId, stepName, StepOutcome.COMPLETED.name(), 1,
                flowBefore, includeFlowStateInLogs ? serializeForLog(flow) : null, null, startedAt);
    }

    /**
     * Execute step only — no advancement. Used in reply mode.
     * The consumer publishes a reply after this returns.
     */
    public void executeStepOnly(String flowId, String stepName) {
        executeStep(flowId, stepName);
    }

    /**
     * Called by the reply consumer after receiving a COMPLETED reply.
     * Uses the flow snapshot from the reply message (not a MongoDB re-read)
     * to avoid the race condition where re-read returns stale data.
     */
    @SuppressWarnings("unchecked")
    public void advanceAfterReply(String flowId, String stepName, String flowSnapshot) {
        F flow;
        if (flowSnapshot != null && !flowSnapshot.isEmpty() && entityClass != null) {
            // Use snapshot from reply — guaranteed to have all domain fields
            try {
                flow = objectMapper.readValue(flowSnapshot, entityClass);
            } catch (Exception e) {
                log.warn("[Saga] Failed to deserialize flow snapshot, falling back to DB: {}",
                        e.getMessage());
                flow = flowRepository.findById(flowId).orElse(null);
            }
        } else {
            // No snapshot (backward compat) — fall back to DB read
            flow = flowRepository.findById(flowId).orElse(null);
        }

        if (flow == null) {
            throw new IllegalArgumentException("Flow not found: " + flowId);
        }
        if (flow.getStatus() == FlowStatus.COMPLETED || flow.getStatus() == FlowStatus.FAILED
                || flow.getStatus() == FlowStatus.CANCELLED || flow.getStatus() == FlowStatus.CANCELLING) return;

        StepHandler<F> handler = stepRegistry.getHandler(stepName);
        markParallelStepCompleted(flow, stepName, handler, stepName);
    }

    /**
     * Publish a reply synchronously to Kafka. Called AFTER saveFlow() completes.
     * Uses .get() to block until broker acknowledges — guarantees the reply
     * consumer reads the flow AFTER domain fields are persisted.
     */
    private void publishReply(String flowId, String stepName, String status,
                              String error, String flowSnapshot) {
        StepReplyMessage reply = StepReplyMessage.builder()
                .flowId(flowId).stepName(stepName)
                .eventId(UUID.randomUUID().toString())
                .status(status).errorMessage(error)
                .flowType(flowType).flowSnapshot(flowSnapshot).build();
        try {
            kafkaTemplate.send(replyTopic, flowId, objectMapper.writeValueAsString(reply)).get();
        } catch (Exception e) {
            log.error("[Saga] Reply publish failed for flow {} step {} — writing outbox fallback: {}",
                    flowId, stepName, e.getMessage());
            try {
                outboxRepository.save(OutboxEvent.builder()
                        .id(UUID.randomUUID().toString())
                        .flowId(flowId).topic(replyTopic).key(flowId)
                        .payload(objectMapper.writeValueAsString(reply))
                        .build());
            } catch (Exception ex) {
                log.error("[Saga] Reply outbox fallback also failed for flow {}: {}", flowId, ex.getMessage());
            }
        }
    }

    public void markDeadLettered(String flowId) {
        markDeadLettered(flowId, null, null);
    }

    public void markDeadLettered(String flowId, String stepName) {
        markDeadLettered(flowId, stepName, null);
    }

    public void markDeadLettered(String flowId, String stepName, String exceptionMessage) {
        String errorDetail = exceptionMessage != null
                ? "[DLT] " + exceptionMessage
                : "[DLT] Exhausted all retry attempts";

        // CAS: only transition to COMPENSATING from non-terminal states.
        // Prevents double compensation if two DLT handlers or recovery scanner race.
        List<String> compensatable = List.of(
                FlowStatus.IN_PROGRESS.name(), FlowStatus.WAITING_RETRY.name(),
                FlowStatus.PARKED.name(), FlowStatus.PENDING.name());

        F flow = casUpdateStatus(flowId, compensatable, FlowStatus.COMPENSATING,
                Map.of("errorMessage", errorDetail));
        if (flow == null) {
            F existing = flowRepository.findById(flowId).orElse(null);
            if (existing == null) {
                log.warn("[DLT] Flow {} not found in database — orphaned Kafka message", flowId);
                logStep(flowId, stepName != null ? stepName : "UNKNOWN", StepOutcome.DEAD_LETTERED.name(), 0,
                        null, null, "[DLT] Flow not found: " + errorDetail, Instant.now());
            } else if (compensatable.contains(existing.getStatus().name())) {
                // Fallback for inline mode (no mongoTemplate)
                existing.setStatus(FlowStatus.COMPENSATING);
                existing.setErrorMessage(errorDetail);
                existing.setUpdatedAt(Instant.now());
                saveFlow(existing);
                flow = existing;
            } else {
                log.info("[DLT] Flow {} already in {} — skipping duplicate compensation", flowId, existing.getStatus());
            }
            if (flow == null) return;
        }
        metrics.flowFailed(flowType);

        logStep(flowId, stepName != null ? stepName : flow.getCurrentStep(),
                StepOutcome.DEAD_LETTERED.name(), flow.getRetryCount(),
                null, null, errorDetail, Instant.now());

        // Run compensation — undo completed steps in reverse
        runCompensation(flow);
    }

    // ========== Signals ==========

    @Setter private SignalRegistry<F> signalRegistry;

    /**
     * Send a signal to a running flow. Temporal-style:
     * - If flow is PARKED/WAITING_RETRY: execute handler immediately, re-publish step
     * - If flow is IN_PROGRESS: queue as pendingSignal, executed after current step completes
     */
    /**
     * Send a signal to a running flow with a typed payload.
     *
     * <pre>
     * orchestrator.signal(flowId, "updatePriority",
     *     PriorityUpdate.builder().priority(Priority.URGENT).reason("escalation").build());
     * </pre>
     *
     * - PARKED/WAITING_RETRY: executes handler immediately, re-publishes step
     * - IN_PROGRESS: queues as pendingSignal, executed after current step completes
     */
    @SuppressWarnings("unchecked")
    public void signal(String flowId, String signalName, Object payload) {
        if (signalRegistry == null) {
            throw new IllegalStateException("No signals registered for flow type " + flowType);
        }
        SignalHandler<F> handler = signalRegistry.getHandler(signalName);
        if (handler == null) {
            throw new IllegalArgumentException("Unknown signal '" + signalName +
                    "'. Available: " + signalRegistry.getSignalNames());
        }

        F flow = flowRepository.findById(flowId).orElse(null);
        if (flow == null) {
            throw new IllegalArgumentException("Flow not found: " + flowId);
        }

        FlowStatus status = flow.getStatus();

        if (status == FlowStatus.PARKED || status == FlowStatus.WAITING_RETRY) {
            // Execute immediately — use saveFlowWithRetry to detect concurrent advancement
            // (scheduler may wake the flow between our read and save)
            Object typedPayload = convertPayload(handler, payload);
            handler.invoke(flow, typedPayload);
            saveFlowWithRetry(flow, flowId);
            log.info("[Signal] Executed '{}' on flow {} (was {})", signalName, flowId, status);

            // Re-publish current step so waitUntil/pollUntil re-evaluates
            try {
                String partitionKey = flow.getCorrelationId() != null
                        ? flow.getCorrelationId() : flow.getId();
                publishStepDirect(flow, flow.getCurrentStep(), partitionKey);
            } catch (Exception e) {
                log.warn("[Signal] Failed to re-publish step after signal: {}", e.getMessage());
            }
        } else if (status == FlowStatus.IN_PROGRESS) {
            // Serialize payload to JSON for MongoDB storage
            String payloadJson = serializePayload(payload);
            var pending = new com.orchestrator.starter.domain.PendingSignal(
                    signalName, payloadJson, Instant.now());

            if (mongoTemplate != null && entityClass != null) {
                // Atomic CAS: only push if flow is still IN_PROGRESS.
                // inc("version") is required because $push bypasses Spring Data's @Version.
                // Without it, completeStep's flowRepository.save() wouldn't detect the
                // concurrent push and would silently overwrite the signal with null.
                // With it, save() throws OptimisticLockingFailureException → retry re-reads
                // the fresh doc → preserves the new signal → drained on next step.
                long modified = mongoTemplate.updateFirst(
                        Query.query(
                                Criteria.where("_id").is(flowId)
                                        .and("status").is(FlowStatus.IN_PROGRESS.name())),
                        new Update()
                                .push("pendingSignals", pending)
                                .inc("version", 1),
                        entityClass).getModifiedCount();
                if (modified > 0) {
                    log.info("[Signal] Queued '{}' on flow {} (IN_PROGRESS)", signalName, flowId);
                } else {
                    // Flow advanced — re-read and execute immediately if now PARKED
                    F freshFlow = flowRepository.findById(flowId).orElse(null);
                    if (freshFlow != null && (freshFlow.getStatus() == FlowStatus.PARKED
                            || freshFlow.getStatus() == FlowStatus.WAITING_RETRY)) {
                        Object typedPayload = convertPayload(handler, payload);
                        handler.invoke(freshFlow, typedPayload);
                        saveFlowWithRetry(freshFlow, flowId);
                        try {
                            String pk = freshFlow.getCorrelationId() != null
                                    ? freshFlow.getCorrelationId() : freshFlow.getId();
                            publishStepDirect(freshFlow, freshFlow.getCurrentStep(), pk);
                        } catch (Exception ex) {
                            log.warn("[Signal] Re-publish after late signal failed: {}", ex.getMessage());
                        }
                        log.info("[Signal] Executed '{}' on flow {} (was IN_PROGRESS, now {})",
                                signalName, flowId, freshFlow.getStatus());
                    } else {
                        log.warn("[Signal] Flow {} changed to {} — signal '{}' dropped",
                                flowId, freshFlow != null ? freshFlow.getStatus() : "NOT_FOUND", signalName);
                    }
                }
            } else {
                var signals = flow.getPendingSignals();
                if (signals == null) {
                    signals = new ArrayList<>();
                    flow.setPendingSignals(signals);
                }
                signals.add(pending);
                saveFlow(flow);
                log.info("[Signal] Queued '{}' on flow {} (in-memory)", signalName, flowId);
            }
        } else {
            log.warn("[Signal] Cannot signal flow {} — status is {}", flowId, status);
        }
    }

    /**
     * Execute all pending signals after a step completes, before advancing.
     *
     * Read-only drain (crash-safe):
     * 1. READ signals from MongoDB (don't clear — if crash, they survive for re-drain)
     * 2. PROCESS each signal handler
     * 3. Set pendingSignals=null in memory — cleared when completeStep saves the flow
     *
     * Concurrent safety: signal $push increments @Version, so completeStep's save
     * detects the conflict and re-reads (preserving newly pushed signals).
     */
    @SuppressWarnings("unchecked")
    private void drainPendingSignals(F flow) {
        if (signalRegistry == null) return;

        List<com.orchestrator.starter.domain.PendingSignal> pending;
        if (mongoTemplate != null && entityClass != null) {
            try {
                F snapshot = (F) mongoTemplate.findById(flow.getId(), entityClass);
                pending = snapshot != null ? snapshot.getPendingSignals() : null;
                // Update in-memory version to match DB (signal push may have incremented it)
                if (snapshot instanceof com.orchestrator.starter.domain.AbstractFlow af
                        && flow instanceof com.orchestrator.starter.domain.AbstractFlow afFlow) {
                    afFlow.setVersion(af.getVersion());
                }
            } catch (Exception e) {
                log.warn("[Signal] Failed to read pendingSignals for flow {}: {}",
                        flow.getId(), e.getMessage());
                return;
            }
        } else {
            pending = flow.getPendingSignals();
        }

        if (pending == null || pending.isEmpty()) return;

        log.info("[Signal] Draining {} pending signal(s) for flow {}", pending.size(), flow.getId());

        for (var ps : pending) {
            SignalHandler<F> handler = signalRegistry.getHandler(ps.getSignalName());
            if (handler == null) {
                log.warn("[Signal] Unknown pending signal '{}' — skipping", ps.getSignalName());
                continue;
            }
            try {
                Object typedPayload = deserializePayload(handler, ps.getPayloadJson());
                if (typedPayload == null && handler.getPayloadType() != null && ps.getPayloadJson() != null) {
                    log.warn("[Signal] Skipping pending '{}' — payload deserialization failed", ps.getSignalName());
                    continue;
                }
                handler.invoke(flow, typedPayload);
                log.info("[Signal] Executed pending '{}' on flow {}", ps.getSignalName(), flow.getId());
            } catch (Exception e) {
                log.error("[Signal] Pending '{}' failed on flow {}: {}",
                        ps.getSignalName(), flow.getId(), e.getMessage());
            }
        }

        flow.setPendingSignals(null);
    }

    /** Convert payload to the handler's expected type via Jackson. */
    private Object convertPayload(SignalHandler<F> handler, Object payload) {
        if (handler.getPayloadType() == null || payload == null) return null;
        if (handler.getPayloadType().isInstance(payload)) return payload;
        try {
            return objectMapper.convertValue(payload, handler.getPayloadType());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot convert signal payload to " + handler.getPayloadType().getSimpleName()
                            + ": " + e.getMessage(), e);
        }
    }

    /** Deserialize queued payload JSON to the handler's expected type. */
    private Object deserializePayload(SignalHandler<F> handler, String payloadJson) {
        if (handler.getPayloadType() == null || payloadJson == null) return null;
        try {
            return objectMapper.readValue(payloadJson, handler.getPayloadType());
        } catch (Exception e) {
            log.warn("[Signal] Failed to deserialize payload to {}: {}",
                    handler.getPayloadType().getSimpleName(), e.getMessage());
            return null;
        }
    }

    private String serializePayload(Object payload) {
        if (payload == null) return null;
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return null;
        }
    }

    // ========== Child Workflow Lifecycle ==========

    /**
     * Re-activate parent flow when a child completes, fails, or is cancelled.
     * No-op if the flow has no parent.
     */
    @SuppressWarnings("unchecked")
    private void notifyParentOnCompletion(F flow) {
        String parentId = flow.getParentFlowId();
        String parentType = flow.getParentFlowType();
        String parentStep = flow.getParentStepName();
        if (parentId == null || parentType == null || parentStep == null) return;

        try {
            FlowTypeDescriptor parentDesc = flowTypeRegistry.get(parentType);
            if (parentDesc == null) {
                log.warn("[Child] Parent flow type '{}' not found — cannot notify", parentType);
                return;
            }
            String partitionKey = parentId;
            com.orchestrator.starter.kafka.StepCommandMessage cmd =
                    com.orchestrator.starter.kafka.StepCommandMessage.builder()
                            .eventId(java.util.UUID.randomUUID().toString())
                            .flowId(parentId)
                            .stepName(parentStep)
                            .flowType(parentType)
                            .build();
            kafkaTemplate.send(parentDesc.getCommandTopic(), partitionKey,
                    objectMapper.writeValueAsString(cmd)).get();
            log.info("[Child] Notified parent {} at step {} (child {} {})",
                    parentId, parentStep, flow.getId(), flow.getStatus());
        } catch (Exception e) {
            log.error("[Child] Failed to notify parent {}: {}", parentId, e.getMessage());
        }
    }

    @Setter private FlowTypeRegistry flowTypeRegistry;

    /**
     * Cancel all child flows when parent is cancelled.
     * No-op if the flow has no children.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void cancelChildFlows(F flow) {
        var childIds = flow.getChildFlowIds();
        if (childIds == null || childIds.isEmpty()) return;

        log.info("[Child] Cascading cancellation to {} children of flow {}", childIds.size(), flow.getId());
        for (String childId : childIds) {
            for (FlowTypeDescriptor desc : flowTypeRegistry.getAll()) {
                var repo = desc.getRepository();
                if (repo == null) continue;
                var childOpt = repo.findById(childId);
                if (childOpt.isPresent()) {
                    FlowOrchestrator childOrch = (FlowOrchestrator) desc.getOrchestrator();
                    childOrch.cancelFlow(childId, "Parent cancelled");
                    break;
                }
            }
        }
    }

    // ========== Cancellation ==========

    /**
     * Cancel a running flow. Runs @OnCancel handlers (or @Compensate fallback)
     * for all completed steps in reverse order, then marks as CANCELLED.
     *
     * Can only cancel flows in IN_PROGRESS, WAITING_RETRY, PARKED, or PENDING status.
     * Returns the cancelled flow, or null if cancellation not allowed.
     */
    public F cancelFlow(String flowId, String reason) {
        String errorMsg = "CANCELLED: " + (reason != null ? reason : "user requested");
        List<String> cancellable = List.of(
                FlowStatus.IN_PROGRESS.name(), FlowStatus.WAITING_RETRY.name(),
                FlowStatus.PARKED.name(), FlowStatus.PENDING.name(), FlowStatus.CANCELLING.name());

        F flow = casUpdateStatus(flowId, cancellable, FlowStatus.CANCELLING,
                Map.of("errorMessage", errorMsg));
        if (flow == null) {
            // CAS failed or no mongoTemplate — try fallback for inline mode
            flow = flowRepository.findById(flowId).orElse(null);
            if (flow == null || !cancellable.contains(flow.getStatus().name())) {
                log.warn("[Saga] Cannot cancel flow {} — status is {}",
                        flowId, flow != null ? flow.getStatus() : "NOT_FOUND");
                return null;
            }
            flow.setStatus(FlowStatus.CANCELLING);
            flow.setErrorMessage(errorMsg);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
        }

        log.info("[Saga] Cancelling flow {} at step {} (reason: {})",
                flowId, flow.getCurrentStep(), reason);

        // Run cancel handlers in reverse for completed steps
        runCancellation(flow);

        return flow;
    }

    private void runCancellation(F flow) {
        List<String> completedSteps = stepRegistry.getCompletedStepsBefore(flow.getCurrentStep());
        // Include current step if it has completed (is in completedSteps set)
        String currentStep = flow.getCurrentStep();
        if (currentStep != null && flow.getCompletedSteps().contains(currentStep)) {
            completedSteps = new ArrayList<>(completedSteps);
            completedSteps.add(currentStep);
        }

        if (completedSteps.isEmpty()) {
            log.info("[Saga] No completed steps to cancel for flow {}", flow.getId());
        } else {
            log.info("[Saga] Running cancellation for flow {} — {} steps to undo",
                    flow.getId(), completedSteps.size());
        }

        // Cancel in reverse order
        for (int i = completedSteps.size() - 1; i >= 0; i--) {
            String stepName = completedSteps.get(i);
            StepHandler<F> handler = stepRegistry.getHandler(stepName);

            if (handler instanceof MethodStepAdapter<F> adapter) {
                Instant start = Instant.now();
                try {
                    adapter.cancel(flow);
                    logStep(flow.getId(), stepName, StepOutcome.CANCELLED.name(), 1, null, null, null, start);
                } catch (Exception e) {
                    log.error("[Saga] Cancel handler failed for step {} on flow {}: {}",
                            stepName, flow.getId(), e.getMessage());
                    logStep(flow.getId(), stepName, StepOutcome.CANCEL_FAILED.name(), 1,
                            null, null, e.getMessage(), start);
                }
            }
        }

        flow.setStatus(FlowStatus.CANCELLED);
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);
        log.info("[Saga] Flow {} cancelled", flow.getId());
        notifyParentOnCompletion(flow);

        // Cascade cancellation to children
        cancelChildFlows(flow);
    }

    // ========== Compensation (what makes this a Saga) ==========

    private void runCompensation(F flow) {
        List<String> completedSteps = stepRegistry.getCompletedStepsBefore(flow.getCurrentStep());
        if (completedSteps.isEmpty()) {
            // No steps to compensate — mark FAILED directly
            flow.setStatus(FlowStatus.FAILED);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
            return;
        }

        log.info("[Saga] Running compensation for flow {} — {} steps to undo",
                flow.getId(), completedSteps.size());

        flow.setStatus(FlowStatus.COMPENSATING);
        saveFlow(flow);

        boolean anyCompensationFailed = false;
        String lastCompensationError = null;

        // Compensate in reverse order
        for (int i = completedSteps.size() - 1; i >= 0; i--) {
            String stepName = completedSteps.get(i);
            StepHandler<F> handler = stepRegistry.getHandler(stepName);

            if (handler instanceof MethodStepAdapter<F> adapter && adapter.hasCompensation()) {
                Instant start = Instant.now();
                try {
                    adapter.compensate(flow);
                    logStep(flow.getId(), stepName, StepOutcome.COMPENSATED.name(), 1, null, null, null, start);
                } catch (Exception e) {
                    anyCompensationFailed = true;
                    lastCompensationError = stepName + ": " + e.getMessage();
                    log.error("[Saga] Compensation failed for step {} on flow {}: {}",
                            stepName, flow.getId(), e.getMessage());
                    logStep(flow.getId(), stepName, StepOutcome.COMPENSATION_FAILED.name(), 1,
                            null, null, e.getMessage(), start);
                }
            } else {
                log.warn("[Saga] No @Compensate for step {}, skipping", stepName);
            }
        }

        if (anyCompensationFailed) {
            flow.setStatus(FlowStatus.COMPENSATION_FAILED);
            flow.setCompensationError(lastCompensationError);
            metrics.compensationFailed(flowType);
        } else {
            flow.setStatus(FlowStatus.FAILED);
        }
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);
    }

    /**
     * Retry compensation for a flow in COMPENSATION_FAILED status.
     * Re-runs runCompensation() which will attempt all compensation handlers again.
     */
    public void retryCompensation(String flowId) {
        F flow = flowRepository.findById(flowId).orElse(null);
        if (flow == null) return;
        if (flow.getStatus() != FlowStatus.COMPENSATION_FAILED
                && flow.getStatus() != FlowStatus.COMPENSATING) return;
        flow.setCompensationError(null);
        runCompensation(flow);
    }

    // ========== Replay ==========

    /** Replay a flow — resume from its current (failed) step. */
    public F replayFlow(String flowId) {
        return replayFlow(flowId, ReplayOptions.builder().build());
    }

    /** Replay a flow from a specific step. */
    public F replayFlow(String flowId, String fromStep) {
        return replayFlow(flowId, ReplayOptions.builder().fromStep(fromStep).build());
    }

    /** Replay a flow with options. */
    @SuppressWarnings("unchecked")
    public F replayFlow(String flowId, ReplayOptions options) {
        F flow = flowRepository.findById(flowId).orElse(null);
        if (flow == null) {
            throw new IllegalArgumentException("Flow not found: " + flowId);
        }

        FlowStatus status = flow.getStatus();

        // COMPLETED requires explicit opt-in
        if (status == FlowStatus.COMPLETED && !options.isAllowCompleted()) {
            throw new IllegalStateException(
                    "Flow " + flowId + " is COMPLETED. Use allowCompleted=true to replay.");
        }

        // Only terminal states can be replayed
        List<String> replayable = new ArrayList<>(List.of(
                FlowStatus.FAILED.name(), FlowStatus.CANCELLED.name(),
                FlowStatus.COMPENSATION_FAILED.name()));
        if (options.isAllowCompleted()) replayable.add(FlowStatus.COMPLETED.name());

        if (!replayable.contains(status.name())) {
            throw new IllegalStateException(
                    "Cannot replay flow " + flowId + " — status is " + status +
                    ". Only FAILED, CANCELLED, COMPENSATION_FAILED, or COMPLETED flows can be replayed.");
        }

        // If fromStep specified, reset completedSteps
        if (options.getFromStep() != null) {
            String fromStep = options.getFromStep();
            // Validate step exists
            if (stepRegistry.getHandler(fromStep) == null) {
                throw new IllegalArgumentException("Unknown step '" + fromStep +
                        "'. Available: " + stepRegistry.getStepNames());
            }
            flow.setCurrentStep(fromStep);
            // Remove fromStep and all subsequent steps from completedSteps
            List<String> stepsToRemove = stepRegistry.getStepsFromInclusive(fromStep);
            flow.getCompletedSteps().removeAll(stepsToRemove);
        }

        var replayFields = resetOrchestrationFields();
        replayFields.put("currentStep", flow.getCurrentStep());
        replayFields.put("completedSteps", flow.getCompletedSteps());

        F updated = casUpdateStatus(flowId, replayable, FlowStatus.IN_PROGRESS, replayFields);
        if (updated != null) {
            flow = updated;
        } else {
            // CAS failed or no mongoTemplate — fallback
            F fresh = flowRepository.findById(flowId).orElse(null);
            if (fresh == null || !replayable.contains(fresh.getStatus().name())) {
                throw new IllegalStateException("Cannot replay flow " + flowId +
                        " — status changed to " + (fresh != null ? fresh.getStatus() : "UNKNOWN"));
            }
            flow = fresh;
            flow.setStatus(FlowStatus.IN_PROGRESS);
            resetOrchestrationState(flow);
            saveFlow(flow);
        }

        // Publish step command to Kafka for immediate execution
        try {
            String partitionKey = flow.getCorrelationId() != null
                    ? flow.getCorrelationId() : flow.getId();
            publishStepDirect(flow, flow.getCurrentStep(), partitionKey);
        } catch (Exception e) {
            log.warn("[Replay] Kafka publish failed for flow {} — recovery scanner will pick it up",
                    flowId, e);
        }

        log.info("[Replay] Flow {} replayed from step {} (was {})",
                flowId, flow.getCurrentStep(), status);
        return flow;
    }

    /** Batch replay — returns per-flow results. */
    public List<Map<String, String>> replayFlows(List<String> flowIds, ReplayOptions options) {
        List<Map<String, String>> results = new ArrayList<>();
        for (String flowId : flowIds) {
            try {
                replayFlow(flowId, options);
                results.add(Map.of("flowId", flowId, "status", "replayed"));
            } catch (Exception e) {
                results.add(Map.of("flowId", flowId, "status", "error", "error", e.getMessage()));
            }
        }
        return results;
    }

    /** Batch cancel — returns per-flow results. */
    public List<Map<String, String>> cancelFlows(List<String> flowIds, String reason) {
        List<Map<String, String>> results = new ArrayList<>();
        for (String flowId : flowIds) {
            try {
                F cancelled = cancelFlow(flowId, reason);
                if (cancelled != null) {
                    results.add(Map.of("flowId", flowId, "status", "cancelled"));
                } else {
                    results.add(Map.of("flowId", flowId, "status", "error",
                            "error", "Flow not in cancellable state"));
                }
            } catch (Exception e) {
                results.add(Map.of("flowId", flowId, "status", "error", "error", e.getMessage()));
            }
        }
        return results;
    }

    // ========== Internal ==========

    private void advanceToNextStep(F flow) {
        advanceToNextStep(flow, flow.getCurrentStep());
    }

    private void advanceToNextStep(F flow, String completedStep) {
        String nextStep = stepRegistry.getNextStep(completedStep);

        if (nextStep == null) {
            // Flow complete — atomic CAS: only complete if still at the completed step
            if (mongoTemplate != null && entityClass != null) {
                long mod = mongoTemplate.updateFirst(
                        Query.query(
                                Criteria
                                        .where("_id").is(flow.getId())
                                        .and("currentStep").is(completedStep)),
                        new Update()
                                .set("status", FlowStatus.COMPLETED.name())
                                .set("updatedAt", Instant.now())
                                .set("completedParallelSteps", List.of()),
                        entityClass
                ).getModifiedCount();
                if (mod == 0) return; // Already completed by another consumer
            } else {
                updateFlowPartial(flow.getId(), Map.of(
                        "status", FlowStatus.COMPLETED.name(),
                        "updatedAt", Instant.now(),
                        "completedParallelSteps", List.of()));
            }
            metrics.flowCompleted(flowType);
            log.info("[Saga] Flow {} completed", flow.getId());
            notifyParentOnCompletion(flow);
            return;
        }

        // Atomic compare-and-swap: only advance if currentStep is still the completed step.
        // If another consumer already advanced (reply consumer vs command consumer race),
        // modifiedCount=0 and we skip — preventing the duplicate cascade.
        long modified = 0;
        if (mongoTemplate != null && entityClass != null) {
            modified = mongoTemplate.updateFirst(
                    Query.query(
                            Criteria
                                    .where("_id").is(flow.getId())
                                    .and("currentStep").is(completedStep)),
                    new Update()
                            .set("currentStep", nextStep)
                            .set("updatedAt", Instant.now()),
                    entityClass
            ).getModifiedCount();
        } else {
            // Fallback for tests without MongoDB
            updateFlowPartial(flow.getId(), Map.of(
                    "currentStep", nextStep, "updatedAt", Instant.now()));
            modified = 1;
        }

        if (modified == 0) {
            log.debug("[Saga] Flow {} already advanced past {} — skipping duplicate send",
                    flow.getId(), completedStep);
            return;
        }

        // We won the race — publish next step command.
        // Direct Kafka publish first (low latency). On failure, write outbox fallback
        // so the outbox publisher delivers it. Recovery scanner also handles the case
        // where crash occurs between DB CAS and Kafka publish (staleThresholdMinutes).
        String partitionKey = flow.getCorrelationId() != null
                ? flow.getCorrelationId() : flow.getId();

        List<String> stepsAtNextOrder = stepRegistry.getStepsAtSameOrder(nextStep);
        List<String> stepsToPublish = stepsAtNextOrder.size() > 1 ? stepsAtNextOrder : List.of(nextStep);

        for (String step : stepsToPublish) {
            try {
                publishStepDirect(flow, step, partitionKey);
            } catch (Exception e) {
                // Direct publish failed — write outbox fallback for guaranteed delivery
                log.warn("[Saga] Direct advance send failed for flow {} step {} — writing outbox fallback: {}",
                        flow.getId(), step, e.getMessage());
                try {
                    StepCommandMessage cmd = StepCommandMessage.builder()
                            .eventId(UUID.randomUUID().toString())
                            .flowId(flow.getId())
                            .correlationId(flow.getCorrelationId())
                            .stepName(step)
                            .flowType(flowType)
                            .build();
                    outboxRepository.save(OutboxEvent.builder()
                            .id(UUID.randomUUID().toString())
                            .flowId(flow.getId())
                            .topic(commandTopic)
                            .key(partitionKey)
                            .payload(objectMapper.writeValueAsString(cmd))
                            .build());
                } catch (Exception ex) {
                    log.error("[Saga] Outbox fallback also failed for flow {} step {}: {}",
                            flow.getId(), step, ex.getMessage());
                }
            }
        }

        if (stepsToPublish.size() > 1) {
            log.info("[Saga] Published {} parallel steps for flow {}: {}",
                    stepsToPublish.size(), flow.getId(), stepsToPublish);
        }
    }

    /** Direct publish to command topic — used by advanceToNextStep for exactly-once delivery. */
    @SuppressWarnings("unchecked")
    private void publishStepDirect(F flow, String stepName, String partitionKey) throws Exception {
        StepCommandMessage cmd = StepCommandMessage.builder()
                .eventId(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .stepName(stepName)
                .flowType(flowType)
                .build();
        kafkaTemplate.send(commandTopic, partitionKey,
                objectMapper.writeValueAsString(cmd)).get();
    }

    /**
     * Partial update via $set — modifies only specified fields.
     * Bypasses @Version check, preserves domain fields.
     * Used by the reply consumer to avoid conflicts with the command consumer.
     */
    private void updateFlowPartial(String flowId, Map<String, Object> fields) {
        if (mongoTemplate != null && entityClass != null) {
            var update = new Update();
            fields.forEach(update::set);
            update.inc("version", 1);
            mongoTemplate.updateFirst(
                    Query.query(
                            Criteria.where("_id").is(flowId)),
                    update, entityClass);
        } else {
            // Fallback: full save (inline mode or no MongoTemplate)
            F flow = flowRepository.findById(flowId).orElse(null);
            if (flow != null) {
                fields.forEach((k, v) -> {
                    if ("status".equals(k)) flow.setStatus(FlowStatus.valueOf((String) v));
                    if ("currentStep".equals(k)) flow.setCurrentStep((String) v);
                    if ("updatedAt".equals(k)) flow.setUpdatedAt((Instant) v);
                });
                flowRepository.save(flow);
            }
        }
    }

    /** Reset all orchestration tracking fields and release execution claim. */
    private void resetOrchestrationState(F flow) {
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setRecoveryCount(0);
        flow.setWaitingSince(null);
        flow.setExpiresAt(null);
        flow.setSleepUntil(null);
        flow.setPollCount(0);
        flow.setCompensationError(null);
        flow.setExecutingStep(null);
        flow.setExecutingPod(null);
        flow.setUpdatedAt(Instant.now());
    }

    /** Returns a field map matching resetOrchestrationState for use with CAS/partial updates. */
    private Map<String, Object> resetOrchestrationFields() {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("retryCount", 0);
        fields.put("backoffSeconds", 0);
        fields.put("nextRetryAt", null);
        fields.put("errorMessage", null);
        fields.put("recoveryCount", 0);
        fields.put("waitingSince", null);
        fields.put("expiresAt", null);
        fields.put("sleepUntil", null);
        fields.put("pollCount", 0);
        fields.put("compensationError", null);
        fields.put("executingStep", null);
        fields.put("executingPod", null);
        return fields;
    }

    /**
     * Complete a step: mark as completed + persist entire flow in one call.
     * Uses flowRepository.save() which is atomic at MongoDB level (single document replace).
     * Domain fields + completedSteps + reset fields all saved together — no inconsistent state.
     */
    private void completeStep(F flow, String flowId, String stepName) {
        flow.getCompletedSteps().add(stepName);
        resetOrchestrationState(flow);
        saveFlowWithRetry(flow, flowId);
    }

    /** Save flow with 3-attempt optimistic lock retry + full $set fallback. */
    @SuppressWarnings("unchecked")
    private void saveFlowWithRetry(F flow, String flowId) {
        boolean saved = false;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                saveFlow(flow);
                saved = true;
                break;
            } catch (org.springframework.dao.OptimisticLockingFailureException e) {
                // Version conflict — typically caused by a concurrent signal $push
                // that incremented version between our read and this save.
                //
                // Re-read fresh doc to get:
                //   - version: so the next save attempt matches DB
                //   - currentStep: in case reply consumer advanced
                //   - pendingSignals: new signals pushed during our step execution
                //     (if we don't preserve these, save() would overwrite them with null)
                log.debug("[Saga] Version conflict saving flow {} (attempt {}), retrying", flowId, attempt + 1);
                F fresh = flowRepository.findById(flowId).orElse(null);
                if (fresh instanceof com.orchestrator.starter.domain.AbstractFlow af
                        && flow instanceof com.orchestrator.starter.domain.AbstractFlow afFlow) {
                    afFlow.setVersion(af.getVersion());
                    afFlow.setCurrentStep(af.getCurrentStep());
                    if (af.getPendingSignals() != null && !af.getPendingSignals().isEmpty()) {
                        afFlow.setPendingSignals(af.getPendingSignals());
                        log.info("[Saga] Preserved {} new signal(s) from concurrent push for flow {}",
                                af.getPendingSignals().size(), flowId);
                    }
                    // completedSteps / completedParallelSteps are append-only sets mutated
                    // concurrently by atomic $addToSet (completeStep on the command side vs
                    // markParallelStepCompleted on the reply side). A full-document save here
                    // would clobber entries the other side just added — UNION them so neither a
                    // step completion nor a parallel-sibling completion is lost (which would
                    // leave a parallel flow stalled forever at the join).
                    if (af.getCompletedSteps() != null) {
                        afFlow.getCompletedSteps().addAll(af.getCompletedSteps());
                    }
                    if (af.getCompletedParallelSteps() != null) {
                        afFlow.getCompletedParallelSteps().addAll(af.getCompletedParallelSteps());
                    }
                }
            }
        }
        if (!saved && mongoTemplate != null && entityClass != null) {
            log.error("[Saga] Version conflict persisted after 3 attempts for flow {} — full partial update", flowId);
            try {
                Map<String, Object> flowMap = objectMapper.convertValue(flow, Map.class);
                flowMap.remove("_id");
                flowMap.remove("id");
                flowMap.remove("version");
                flowMap.put("updatedAt", Instant.now());
                var update = new Update();
                flowMap.forEach(update::set);
                update.inc("version", 1);
                mongoTemplate.updateFirst(
                        Query.query(
                                Criteria.where("_id").is(flowId)),
                        update, entityClass);
            } catch (Exception ex) {
                log.error("[Saga] Full partial update also failed for flow {}: {}", flowId, ex.getMessage());
            }
        }
    }

    /**
     * Handle unexpected exceptions via @RecoverOn / @RetryOn / @FailOn.
     * @return true if recovered (step should be marked completed), false if permanently failed
     * @throws RetryableStepException if the error is retryable
     */
    private boolean handleUnexpectedStepError(StepHandler<F> handler, F flow, String flowId,
                                               String stepName, String flowBefore, Instant startedAt, Exception e) {
        try {
            StepErrorHandler.handleError(handler, e);
            logStep(flowId, stepName, StepOutcome.RECOVERED.name(), flow.getRetryCount() + 1,
                    flowBefore, includeFlowStateInLogs ? serializeForLog(flow) : null, e.getMessage(), startedAt);
            log.info("[Saga] Step {} recovered for flow {}", stepName, flowId);
            return true;
        } catch (RetryableStepException re) {
            logStep(flowId, stepName, StepOutcome.RETRYING.name(), flow.getRetryCount() + 1,
                    flowBefore, null, re.getMessage(), startedAt);
            handleRetryableFailure(flow, re);
            throw re;
        } catch (NonRetryableStepException nre) {
            logStep(flowId, stepName, StepOutcome.FAILED.name(), flow.getRetryCount() + 1,
                    flowBefore, null, nre.getMessage(), startedAt);
            handlePermanentFailure(flow, nre);
            return false;
        }
    }

    private void handleWaitingStep(F flow, WaitingStepException e) {
        String errorMsg = e.getMessage() != null ? e.getMessage() : "waiting for external event";
        boolean isSleeping = e.getWaitMode() == WaitingStepException.WaitMode.SLEEPING;
        // SLEEPING uses PARKED status but with nextRetryAt (scheduler wakes it)
        FlowStatus targetStatus = (e.isParked() || isSleeping) ? FlowStatus.PARKED : FlowStatus.WAITING_RETRY;
        Instant now = Instant.now();

        if (mongoTemplate != null && entityClass != null) {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("status", targetStatus.name());
            fields.put("errorMessage", errorMsg);
            fields.put("executingStep", null);
            fields.put("executingPod", null);
            fields.put("updatedAt", now);
            // Polling and sleeping: set nextRetryAt so scheduler re-delivers
            if (e.getWaitMode() == WaitingStepException.WaitMode.POLLING && e.getPollInterval() != null) {
                fields.put("nextRetryAt", now.plus(e.getPollInterval()));
                fields.put("pollCount", flow.getPollCount() + 1);
            } else if (isSleeping) {
                fields.put("nextRetryAt", now.plus(e.getExpiry()));
            }
            updateFlowPartial(flow.getId(), fields);

            // Atomic CAS: set waitingSince/expiresAt only if not already set in DB.
            // Prevents concurrent re-entry from resetting the original expiry deadline.
            Update firstEntryUpdate = new Update()
                    .set("waitingSince", now);
            if (!isSleeping) {
                firstEntryUpdate.set("expiresAt", now.plus(e.getExpiry()));
            }
            mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(flow.getId())
                            .and("waitingSince").is(null)),
                    firstEntryUpdate, entityClass);
        } else {
            flow.setStatus(targetStatus);
            flow.setErrorMessage(errorMsg);
            flow.setUpdatedAt(now);
            if (flow.getWaitingSince() == null) {
                flow.setWaitingSince(now);
                if (!isSleeping) {
                    flow.setExpiresAt(now.plus(e.getExpiry()));
                }
            }
            if (e.getWaitMode() == WaitingStepException.WaitMode.POLLING && e.getPollInterval() != null) {
                flow.setNextRetryAt(now.plus(e.getPollInterval()));
                flow.setPollCount(flow.getPollCount() + 1);
            } else if (isSleeping) {
                flow.setNextRetryAt(now.plus(e.getExpiry()));
            }
            saveFlow(flow);
        }
    }

    /**
     * Jittered exponential backoff (equal jitter) using the configured orchestrator.retry.* values.
     * Mirrors JitteredExponentialBackOffPolicy so the WAITING_RETRY retry path — which is the path
     * actually used in failover mode (and by the scanner-driven redelivery) — spreads retries the
     * same way the non-blocking retry topics do, preventing a thundering herd.
     */
    private long jitteredBackoffMs(int attempt) {
        double base = Math.min(
                retryInitialIntervalMs * Math.pow(retryMultiplier, Math.max(0, attempt - 1)),
                retryMaxIntervalMs);
        double jitter = Math.max(0.0, Math.min(1.0, retryJitterFactor));
        // equal jitter: fixed (1-jitter) portion + random [0, jitter) portion
        return (long) (base * (1 - jitter)
                + java.util.concurrent.ThreadLocalRandom.current().nextDouble() * base * jitter);
    }

    private void handleRetryableFailure(F flow, RetryableStepException e) {
        int retryCount = flow.getRetryCount() + 1;
        long backoffMs = jitteredBackoffMs(retryCount);
        int backoff = (int) Math.max(1, backoffMs / 1000);
        Instant nextRetry = Instant.now().plusMillis(backoffMs);
        String errorMsg = e.getMessage() != null ? e.getMessage() : "retryable error";
        if (mongoTemplate != null && entityClass != null) {
            // Kafka mode: the NON-BLOCKING RETRY TOPICS own retryable redelivery — the rethrown
            // exception routes this message to retry-0..N with the configured jittered backoff,
            // bounded by retry.max-attempts, then DLT → markDeadLettered.
            // nextRetryAt is explicitly NULLED (not just omitted — it clears any stale value):
            //  - a set nextRetryAt makes redeliverPollingFlows re-publish FRESH main-topic messages
            //    whose retry headers start at attempt 1 → the attempt count never accumulates →
            //    unbounded retries;
            //  - and the skip-guard (WAITING_RETRY + future nextRetryAt) swallows the retry-topic
            //    chain's own redeliveries → the bounded chain starves.
            // WAITING_RETRY + retryCount are kept for observability; the scanner keeps owning
            // pollUntil/sleep (handleWaitingStep sets nextRetryAt) and crash recovery (IN_PROGRESS).
            var fields = new LinkedHashMap<String, Object>();
            fields.put("retryCount", retryCount);
            fields.put("backoffSeconds", 0);
            fields.put("nextRetryAt", null);
            fields.put("status", FlowStatus.WAITING_RETRY.name());
            fields.put("errorMessage", errorMsg);
            fields.put("executingStep", null);
            fields.put("executingPod", null);
            fields.put("updatedAt", Instant.now());
            updateFlowPartial(flow.getId(), fields);
        } else {
            flow.setRetryCount(retryCount);
            flow.setBackoffSeconds(backoff);
            flow.setNextRetryAt(nextRetry);
            flow.setStatus(FlowStatus.WAITING_RETRY);
            flow.setErrorMessage(errorMsg);
            flow.setExecutingStep(null);
            flow.setExecutingPod(null);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
        }
    }

    private void handlePermanentFailure(F flow, NonRetryableStepException e) {
        metrics.flowFailed(flowType);
        String errorMsg = e.getMessage() != null ? e.getMessage() : "permanent failure";

        // Set COMPENSATING + error message first — if crash during compensation,
        // recovery scanner will detect COMPENSATING and re-run (Fix S1/S5)
        flow.setStatus(FlowStatus.COMPENSATING);
        flow.setErrorMessage(errorMsg);
        flow.setExecutingStep(null);
        flow.setExecutingPod(null);
        flow.setUpdatedAt(Instant.now());
        if (mongoTemplate != null && entityClass != null) {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("status", FlowStatus.COMPENSATING.name());
            fields.put("errorMessage", errorMsg);
            fields.put("executingStep", null);
            fields.put("executingPod", null);
            fields.put("updatedAt", Instant.now());
            updateFlowPartial(flow.getId(), fields);
        } else {
            saveFlow(flow);
        }

        // Run compensation — sets final status (FAILED or COMPENSATION_FAILED) and saves
        runCompensation(flow);
    }

    /**
     * After a parallel step completes, track it and check if all siblings are done.
     * If all done, advance to the next step (which may be a @JoinOn step).
     */
    private void markParallelStepCompleted(F flow, String stepName, StepHandler<F> handler) {
        markParallelStepCompleted(flow, stepName, handler, stepName);
    }

    @SuppressWarnings("unchecked")
    private void markParallelStepCompleted(F flow, String stepName, StepHandler<F> handler,
                                            String completedStep) {
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isParallel()) {
            // Atomic $addToSet — prevents race where two parallel steps save independently
            // and neither sees the other's completion in their in-memory set.
            if (mongoTemplate != null && entityClass != null) {
                mongoTemplate.updateFirst(
                        Query.query(Criteria.where("_id").is(flow.getId())),
                        new Update()
                                .addToSet("completedParallelSteps", stepName)
                                .set("updatedAt", Instant.now())
                                .inc("version", 1),
                        entityClass);
                // Re-read from DB to get the authoritative set (includes other threads' additions)
                F fresh = flowRepository.findById(flow.getId()).orElse(null);
                if (fresh == null) return;
                Set<String> completed = fresh.getCompletedParallelSteps();
                List<StepHandler<F>> siblings = stepRegistry.getParallelGroup(adapter.getParallelGroup());
                boolean allDone = siblings.stream().allMatch(s -> completed.contains(s.getStepName()));

                if (allDone) {
                    log.info("[Saga] All parallel steps in group '{}' completed for flow {}",
                            adapter.getParallelGroup(), flow.getId());
                    // Advance using the group's ACTUAL currentStep (pinned to the first
                    // parallel sibling when the group was dispatched) — not the last-completed
                    // step, whose name won't match the currentStep CAS in advanceToNextStep
                    // and would leave the flow stalled at the join. getNextStep maps every
                    // sibling to the same join, so this resolves correctly regardless of
                    // which sibling finished last.
                    advanceToNextStep(flow, fresh.getCurrentStep());
                } else {
                    log.info("[Saga] Parallel step {} done, waiting for siblings in group '{}'",
                            stepName, adapter.getParallelGroup());
                }
            } else {
                // Fallback for inline mode
                Set<String> completed = new java.util.HashSet<>(flow.getCompletedParallelSteps());
                completed.add(stepName);
                flow.setCompletedParallelSteps(completed);
                flow.setUpdatedAt(Instant.now());
                saveFlow(flow);

                List<StepHandler<F>> siblings = stepRegistry.getParallelGroup(adapter.getParallelGroup());
                boolean allDone = siblings.stream().allMatch(s -> completed.contains(s.getStepName()));
                if (allDone) {
                    // Advance from the group's actual currentStep, not the last-completed
                    // step (see the mongoTemplate branch above for the rationale).
                    advanceToNextStep(flow, flow.getCurrentStep());
                }
            }
        } else {
            // Sequential step — advance using the completed step name for correct next-step resolution
            advanceToNextStep(flow, completedStep);
        }
    }

    private void writeOutboxEvent(F flow) {
        writeOutboxEvent(flow, flow.getCurrentStep());
    }

    private void writeOutboxEvent(F flow, String stepNameOverride) {
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .correlationId(flow.getCorrelationId())
                    .stepName(stepNameOverride)
                    .flowType(flowType)
                    .build();

            String partitionKey = flow.getCorrelationId() != null
                    ? flow.getCorrelationId() : flow.getId();
            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .topic(commandTopic)
                    .key(partitionKey)
                    .payload(objectMapper.writeValueAsString(cmd))
                    .build();

            outboxRepository.save(event);
        } catch (Exception e) {
            log.error("[Saga] Failed to write outbox event: {}", e.getMessage());
            throw new RuntimeException("Failed to write outbox event for flow " + flow.getId(), e);
        }
    }

    private void logStep(String flowId, String stepName, String status, int attempt,
                         String before, String after, String error, Instant startedAt) {
        try {
            Instant now = Instant.now();
            stepLogRepository.save(StepExecutionLog.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .parentFlowId(MDC.get("parentFlowId"))
                    .stepName(stepName)
                    .status(status)
                    .attemptNumber(attempt)
                    .flowStateBefore(before)
                    .flowStateAfter(after)
                    .errorMessage(error)
                    .durationMs(now.toEpochMilli() - startedAt.toEpochMilli())
                    .startedAt(startedAt)
                    .completedAt(now)
                    .build());
        } catch (Exception e) {
            log.warn("[Saga] Failed to log step execution: {}", e.getMessage());
        }
    }

    /**
     * Runs the given action in a MongoDB transaction if available.
     * On standalone MongoDB (no TransactionTemplate): runs without transaction.
     * On replica set with transactions enabled: fully atomic.
     *
     * The user never touches this — the library handles it internally
     * for all flow save + outbox event writes.
     */
    private <R> R runInTransaction(java.util.function.Supplier<R> action, R fallback) {
        if (txTemplate != null) {
            return txTemplate.execute(status -> action.get());
        }
        return action.get();
    }

    /**
     * Atomic CAS status transition via MongoDB updateFirst.
     * Only updates if the current status matches one of {@code fromStatuses}.
     * Returns the updated flow on success, null if CAS failed (status already changed).
     *
     * @param extraFields additional fields to $set atomically with the status change (may be null)
     */
    @SuppressWarnings("unchecked")
    private F casUpdateStatus(String flowId, List<String> fromStatuses,
                               FlowStatus toStatus, Map<String, Object> extraFields) {
        if (mongoTemplate == null || entityClass == null) return null;
        Update update = new Update()
                .set("status", toStatus.name())
                .set("updatedAt", Instant.now())
                .inc("version", 1);
        if (extraFields != null) {
            extraFields.forEach(update::set);
        }
        long modified = mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId).and("status").in(fromStatuses)),
                update, entityClass).getModifiedCount();
        if (modified == 0) return null;
        return flowRepository.findById(flowId).orElse(null);
    }

    /**
     * Save with optimistic lock retry (up to 3 attempts).
     * Concurrent reply consumers may increment the version between our read
     * and save. On conflict, re-read the latest version and retry.
     */



    /**
     * Execute a step handler with an optional timeout.
     * When timeout is enabled (> 0), runs the handler on a virtual thread
     * and throws RetryableStepException if it exceeds the configured duration.
     */
    private void executeWithTimeout(StepHandler<F> handler, F flow, String stepName) throws Exception {
        if (stepTimeoutSeconds <= 0 || stepExecutor == null) {
            handler.execute(flow);
            return;
        }
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                handler.execute(flow);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, stepExecutor);
        try {
            future.get(stepTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true); // Interrupt the virtual thread to release resources
            throw new RetryableStepException(
                    "Step " + stepName + " timed out after " + stepTimeoutSeconds + "s");
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Exception ex) throw ex;
            throw new RuntimeException(cause);
        }
    }

    /**
     * Persist the flow. Concurrency is handled by Kafka partition key
     * (same flowId → same partition → same consumer) and two-layer idempotency,
     * not by optimistic locking. This avoids version conflicts between
     * coordinated command and reply consumers.
     */
    private void saveFlow(F flow) {
        flowRepository.save(flow);
    }

    /**
     * Search flows by @SearchAttribute fields.
     * Builds a MongoDB query from the provided key-value pairs.
     */
    public List<F> findFlows(Map<String, Object> searchAttributes) {
        if (mongoTemplate == null || entityClass == null) {
            log.warn("[Search] findFlows called but mongoTemplate/entityClass not configured — returning empty");
            return List.of();
        }
        if (searchAttributes == null || searchAttributes.isEmpty()) {
            return List.of();
        }
        var criteria = new Criteria();
        var criteriaList = new java.util.ArrayList<Criteria>();
        searchAttributes.forEach((k, v) ->
                criteriaList.add(Criteria.where(k).is(v)));
        var query = Query.query(
                new Criteria().andOperator(
                        criteriaList.toArray(new Criteria[0])));
        query.limit(100);
        return mongoTemplate.find(query, entityClass);
    }

    private String serialize(F flow) {
        try {
            String json = objectMapper.writeValueAsString(flow);
            return json;
        } catch (Exception e) {
            return "{}";
        }
    }

    /** Serialize for step logs — skips if result exceeds max size to avoid storing binary blobs. */
    private String serializeForLog(F flow) {
        try {
            String json = objectMapper.writeValueAsString(flow);
            if (json.length() > maxLogSnapshotBytes) {
                log.debug("[Saga] Flow state too large for step log ({} bytes > {} max), skipping",
                        json.length(), maxLogSnapshotBytes);
                return null;
            }
            return json;
        } catch (Exception e) {
            return null;
        }
    }

    /** Shutdown the step executor to prevent thread leaks. */
    public void shutdown() {
        if (stepExecutor != null) {
            stepExecutor.shutdown();
        }
    }
}
