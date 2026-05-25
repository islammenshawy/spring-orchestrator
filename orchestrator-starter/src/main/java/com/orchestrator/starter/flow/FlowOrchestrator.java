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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
        MDC.put("flowType", flowType != null ? flowType : "default");
        if (stepName != null) MDC.put("stepName", stepName);
        try {
        doExecuteStepInner(flowId, stepName);
        } finally {
            MDC.remove("flowId");
            MDC.remove("flowType");
            MDC.remove("stepName");
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

        // Use the step name from the Kafka message (supports parallel steps)
        if (stepName == null) stepName = flow.getCurrentStep();
        StepHandler<F> handler = stepRegistry.getHandler(stepName);

        // Check if this is a join point — all parallel steps must be done first
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isJoinPoint()) {
            String group = adapter.getJoinOnGroup();
            List<StepHandler<F>> parallelSteps = stepRegistry.getParallelGroup(group);
            boolean allDone = parallelSteps.stream()
                    .allMatch(ps -> flow.getCompletedSteps().contains(ps.getStepName()));
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

        flow.setStatus(FlowStatus.IN_PROGRESS);
        String flowBefore = includeFlowStateInLogs ? serializeForLog(flow) : null;
        Instant startedAt = Instant.now();

        log.info("[Saga] Executing step {} for flow {}", stepName, flowId);

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

        // Step succeeded — mark completed and persist
        markStepCompleted(flow, stepName);
        saveFlowWithRetry(flow, flowId);

        // Drain pending signals before advancing (Temporal-style: signals execute between steps)
        drainPendingSignals(flow);

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
        try {
            StepReplyMessage reply = StepReplyMessage.builder()
                    .flowId(flowId)
                    .stepName(stepName)
                    .eventId(UUID.randomUUID().toString())
                    .status(status)
                    .errorMessage(error)
                    .flowType(flowType)
                    .flowSnapshot(flowSnapshot)
                    .build();
            // Reply uses flowId as key — reply consumer always runs on same instance
            kafkaTemplate.send(replyTopic, flowId, objectMapper.writeValueAsString(reply)).get();
        } catch (Exception e) {
            log.error("[Saga] Reply publish failed for flow {} step {} — writing outbox fallback: {}",
                    flowId, stepName, e.getMessage());
            // Fallback: write reply as outbox event so it gets retried
            try {
                String partitionKey = flowId;
                StepReplyMessage reply = StepReplyMessage.builder()
                        .flowId(flowId).stepName(stepName)
                        .eventId(UUID.randomUUID().toString())
                        .status(status).errorMessage(error)
                        .flowType(flowType).flowSnapshot(flowSnapshot).build();
                outboxRepository.save(com.orchestrator.starter.outbox.OutboxEvent.builder()
                        .id(UUID.randomUUID().toString())
                        .flowId(flowId)
                        .topic(replyTopic)
                        .key(partitionKey)
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
        var optFlow = flowRepository.findById(flowId);
        if (optFlow.isEmpty()) {
            log.warn("[DLT] Flow {} not found in database — orphaned Kafka message", flowId);
            logStep(flowId, stepName != null ? stepName : "UNKNOWN", StepOutcome.DEAD_LETTERED.name(), 0,
                    null, null, "[DLT] Flow not found: " + (exceptionMessage != null ? exceptionMessage : "orphaned message"), Instant.now());
            return;
        }

        F flow = optFlow.get();
        String errorDetail = exceptionMessage != null
                ? "[DLT] " + exceptionMessage
                : "[DLT] Exhausted all retry attempts";
        flow.setStatus(FlowStatus.FAILED);
        flow.setErrorMessage(errorDetail);
        flow.setUpdatedAt(Instant.now());
        metrics.flowFailed(flowType);
        saveFlow(flow);

        logStep(flowId, stepName != null ? stepName : flow.getCurrentStep(),
                StepOutcome.DEAD_LETTERED.name(), flow.getRetryCount(),
                null, null, errorDetail, Instant.now());

        // Run compensation for all completed steps in reverse
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
            // Safe to execute immediately — nothing else is running
            Object typedPayload = convertPayload(handler, payload);
            handler.invoke(flow, typedPayload);
            saveFlow(flow);
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
                // Atomic CAS: only push if flow is still IN_PROGRESS
                // If flow advanced between our read and this write, modifiedCount=0
                long modified = mongoTemplate.updateFirst(
                        org.springframework.data.mongodb.core.query.Query.query(
                                org.springframework.data.mongodb.core.query.Criteria.where("_id").is(flowId)
                                        .and("status").is(FlowStatus.IN_PROGRESS.name())),
                        new org.springframework.data.mongodb.core.query.Update()
                                .push("pendingSignals", pending),
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
                        saveFlow(freshFlow);
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
                    signals = new java.util.ArrayList<>();
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
     */
    @SuppressWarnings("unchecked")
    private void drainPendingSignals(F flow) {
        if (signalRegistry == null) return;

        // Atomic read-and-clear: findAndModify returns the OLD pendingSignals
        // and unsets them in a single MongoDB operation. Zero race window:
        // - Signals $pushed BEFORE this → in the returned list → drained
        // - Signals $pushed AFTER this → create a new array → survive for next drain
        java.util.List<com.orchestrator.starter.domain.PendingSignal> pending;
        if (mongoTemplate != null && entityClass != null) {
            try {
                F snapshot = (F) mongoTemplate.findAndModify(
                        org.springframework.data.mongodb.core.query.Query.query(
                                org.springframework.data.mongodb.core.query.Criteria.where("_id").is(flow.getId())
                                        .and("pendingSignals").ne(null)),
                        new org.springframework.data.mongodb.core.query.Update().unset("pendingSignals"),
                        org.springframework.data.mongodb.core.FindAndModifyOptions.options().returnNew(false),
                        entityClass);
                pending = snapshot != null ? snapshot.getPendingSignals() : null;
            } catch (Exception e) {
                // findAndModify failed — signals stay in MongoDB, will be drained on next step
                log.warn("[Signal] Drain findAndModify failed for flow {} — signals preserved for next drain: {}",
                        flow.getId(), e.getMessage());
                return;
            }
        } else {
            pending = flow.getPendingSignals();
            flow.setPendingSignals(null);
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
                handler.invoke(flow, typedPayload);
                log.info("[Signal] Executed pending '{}' on flow {}", ps.getSignalName(), flow.getId());
            } catch (Exception e) {
                log.error("[Signal] Pending '{}' failed on flow {}: {}",
                        ps.getSignalName(), flow.getId(), e.getMessage());
            }
        }
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
        F flow = flowRepository.findById(flowId).orElse(null);
        if (flow == null) return null;

        FlowStatus status = flow.getStatus();
        if (status != FlowStatus.IN_PROGRESS && status != FlowStatus.WAITING_RETRY
                && status != FlowStatus.PARKED && status != FlowStatus.PENDING) {
            log.warn("[Saga] Cannot cancel flow {} — status is {}", flowId, status);
            return null;
        }

        log.info("[Saga] Cancelling flow {} at step {} (reason: {})",
                flowId, flow.getCurrentStep(), reason);

        flow.setStatus(FlowStatus.CANCELLING);
        flow.setErrorMessage("CANCELLED: " + (reason != null ? reason : "user requested"));
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);

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
        if (completedSteps.isEmpty()) return;

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
        if (flow == null || flow.getStatus() != FlowStatus.COMPENSATION_FAILED) return;
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
        if (status != FlowStatus.FAILED && status != FlowStatus.CANCELLED
                && status != FlowStatus.COMPENSATION_FAILED && status != FlowStatus.COMPLETED) {
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

        // Reset orchestration state
        flow.setStatus(FlowStatus.IN_PROGRESS);
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setRecoveryCount(0);
        flow.setWaitingSince(null);
        flow.setExpiresAt(null);
        flow.setSleepUntil(null);
        flow.setCompensationError(null);
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);

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
    public List<java.util.Map<String, String>> replayFlows(List<String> flowIds, ReplayOptions options) {
        List<java.util.Map<String, String>> results = new java.util.ArrayList<>();
        for (String flowId : flowIds) {
            try {
                replayFlow(flowId, options);
                results.add(java.util.Map.of("flowId", flowId, "status", "replayed"));
            } catch (Exception e) {
                results.add(java.util.Map.of("flowId", flowId, "status", "error", "error", e.getMessage()));
            }
        }
        return results;
    }

    /** Batch cancel — returns per-flow results. */
    public List<java.util.Map<String, String>> cancelFlows(List<String> flowIds, String reason) {
        List<java.util.Map<String, String>> results = new java.util.ArrayList<>();
        for (String flowId : flowIds) {
            try {
                F cancelled = cancelFlow(flowId, reason);
                if (cancelled != null) {
                    results.add(java.util.Map.of("flowId", flowId, "status", "cancelled"));
                } else {
                    results.add(java.util.Map.of("flowId", flowId, "status", "error",
                            "error", "Flow not in cancellable state"));
                }
            } catch (Exception e) {
                results.add(java.util.Map.of("flowId", flowId, "status", "error", "error", e.getMessage()));
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
                        org.springframework.data.mongodb.core.query.Query.query(
                                org.springframework.data.mongodb.core.query.Criteria
                                        .where("_id").is(flow.getId())
                                        .and("currentStep").is(completedStep)),
                        new org.springframework.data.mongodb.core.query.Update()
                                .set("status", FlowStatus.COMPLETED.name())
                                .set("updatedAt", Instant.now())
                                .set("completedParallelSteps", java.util.List.of()),
                        entityClass
                ).getModifiedCount();
                if (mod == 0) return; // Already completed by another consumer
            } else {
                updateFlowPartial(flow.getId(), java.util.Map.of(
                        "status", FlowStatus.COMPLETED.name(),
                        "updatedAt", Instant.now(),
                        "completedParallelSteps", java.util.List.of()));
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
                    org.springframework.data.mongodb.core.query.Query.query(
                            org.springframework.data.mongodb.core.query.Criteria
                                    .where("_id").is(flow.getId())
                                    .and("currentStep").is(completedStep)),
                    new org.springframework.data.mongodb.core.query.Update()
                            .set("currentStep", nextStep)
                            .set("updatedAt", Instant.now()),
                    entityClass
            ).getModifiedCount();
        } else {
            // Fallback for tests without MongoDB
            updateFlowPartial(flow.getId(), java.util.Map.of(
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
    private void updateFlowPartial(String flowId, java.util.Map<String, Object> fields) {
        if (mongoTemplate != null && entityClass != null) {
            var update = new org.springframework.data.mongodb.core.query.Update();
            fields.forEach(update::set);
            update.inc("version", 1);
            mongoTemplate.updateFirst(
                    org.springframework.data.mongodb.core.query.Query.query(
                            org.springframework.data.mongodb.core.query.Criteria.where("_id").is(flowId)),
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

    /**
     * Gate/polling step — parks flow without incrementing retryCount.
     *
     * PARKED: flow sleeps in MongoDB (status=PARKED). No Kafka re-delivery.
     *         Woken by external trigger (webhook/API re-publishes step command).
     * POLLING: flow waits for nextRetryAt (status=WAITING_RETRY). Scheduler
     *          re-delivers when the poll interval elapses.
     */
    /** Mark step as completed and clear all retry/recovery state. */
    private void markStepCompleted(F flow, String stepName) {
        flow.getCompletedSteps().add(stepName);
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setRecoveryCount(0);
        flow.setWaitingSince(null);
        flow.setExpiresAt(null);
        flow.setSleepUntil(null);
        flow.setUpdatedAt(Instant.now());
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
                log.debug("[Saga] Version conflict saving flow {} (attempt {}), retrying", flowId, attempt + 1);
                F fresh = flowRepository.findById(flowId).orElse(null);
                if (fresh instanceof com.orchestrator.starter.domain.AbstractFlow af
                        && flow instanceof com.orchestrator.starter.domain.AbstractFlow afFlow) {
                    afFlow.setVersion(af.getVersion());
                    afFlow.setCurrentStep(af.getCurrentStep());
                }
            }
        }
        if (!saved && mongoTemplate != null && entityClass != null) {
            log.error("[Saga] Version conflict persisted after 3 attempts for flow {} — full partial update", flowId);
            try {
                java.util.Map<String, Object> flowMap = objectMapper.convertValue(flow, java.util.Map.class);
                flowMap.remove("_id");
                flowMap.remove("id");
                flowMap.remove("version");
                flowMap.put("updatedAt", Instant.now());
                var update = new org.springframework.data.mongodb.core.query.Update();
                flowMap.forEach(update::set);
                update.inc("version", 1);
                mongoTemplate.updateFirst(
                        org.springframework.data.mongodb.core.query.Query.query(
                                org.springframework.data.mongodb.core.query.Criteria.where("_id").is(flowId)),
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
            var fields = new java.util.LinkedHashMap<String, Object>();
            fields.put("status", targetStatus.name());
            fields.put("errorMessage", errorMsg);
            fields.put("updatedAt", now);
            // Set waitingSince and expiresAt only on first entry — don't reset on re-activation
            if (flow.getWaitingSince() == null) {
                fields.put("waitingSince", now);
                // SLEEPING has no expiry — the sleep IS the intended wait
                if (!isSleeping) {
                    fields.put("expiresAt", now.plus(e.getExpiry()));
                }
            }
            // Polling and sleeping: set nextRetryAt so scheduler re-delivers
            if (e.getWaitMode() == WaitingStepException.WaitMode.POLLING && e.getPollInterval() != null) {
                fields.put("nextRetryAt", now.plus(e.getPollInterval()));
            } else if (isSleeping) {
                fields.put("nextRetryAt", now.plus(e.getExpiry()));
            }
            updateFlowPartial(flow.getId(), fields);
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
            } else if (isSleeping) {
                flow.setNextRetryAt(now.plus(e.getExpiry()));
            }
            saveFlow(flow);
        }
    }

    private void handleRetryableFailure(F flow, RetryableStepException e) {
        int retryCount = flow.getRetryCount() + 1;
        int backoff = (int) Math.min(Math.pow(2, retryCount), 60);
        Instant nextRetry = Instant.now().plusSeconds(backoff);
        String errorMsg = e.getMessage() != null ? e.getMessage() : "retryable error";
        // Use $set to avoid @Version conflict
        if (mongoTemplate != null && entityClass != null) {
            updateFlowPartial(flow.getId(), java.util.Map.of(
                    "retryCount", retryCount,
                    "backoffSeconds", backoff,
                    "nextRetryAt", nextRetry,
                    "status", FlowStatus.WAITING_RETRY.name(),
                    "errorMessage", errorMsg,
                    "updatedAt", Instant.now()));
        } else {
            flow.setRetryCount(retryCount);
            flow.setBackoffSeconds(backoff);
            flow.setNextRetryAt(nextRetry);
            flow.setStatus(FlowStatus.WAITING_RETRY);
            flow.setErrorMessage(errorMsg);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
        }
    }

    private void handlePermanentFailure(F flow, NonRetryableStepException e) {
        metrics.flowFailed(flowType);
        String errorMsg = e.getMessage() != null ? e.getMessage() : "permanent failure";
        if (mongoTemplate != null && entityClass != null) {
            updateFlowPartial(flow.getId(), java.util.Map.of(
                    "status", FlowStatus.FAILED.name(),
                    "errorMessage", errorMsg,
                    "updatedAt", Instant.now()));
        } else {
            flow.setStatus(FlowStatus.FAILED);
            flow.setErrorMessage(errorMsg);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
        }
        // Saga compensation — undo completed steps in reverse
        runCompensation(flow);
    }

    /**
     * After a parallel step completes, track it and check if all siblings are done.
     * If all done, advance to the next step (which may be a @JoinOn step).
     */
    private void markParallelStepCompleted(F flow, String stepName, StepHandler<F> handler) {
        markParallelStepCompleted(flow, stepName, handler, stepName);
    }

    private void markParallelStepCompleted(F flow, String stepName, StepHandler<F> handler,
                                            String completedStep) {
        if (handler instanceof MethodStepAdapter<?> adapter && adapter.isParallel()) {
            java.util.Set<String> completed = new java.util.HashSet<>(flow.getCompletedParallelSteps());
            completed.add(stepName);
            flow.setCompletedParallelSteps(completed);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);

            List<StepHandler<F>> siblings = stepRegistry.getParallelGroup(adapter.getParallelGroup());
            boolean allDone = siblings.stream().allMatch(s -> completed.contains(s.getStepName()));

            if (allDone) {
                log.info("[Saga] All parallel steps in group '{}' completed for flow {}",
                        adapter.getParallelGroup(), flow.getId());
                advanceToNextStep(flow, completedStep);
            } else {
                log.info("[Saga] Parallel step {} done, waiting for siblings in group '{}'",
                        stepName, adapter.getParallelGroup());
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
    public List<F> findFlows(java.util.Map<String, Object> searchAttributes) {
        if (mongoTemplate == null || entityClass == null || searchAttributes == null || searchAttributes.isEmpty()) {
            return List.of();
        }
        var criteria = new org.springframework.data.mongodb.core.query.Criteria();
        var criteriaList = new java.util.ArrayList<org.springframework.data.mongodb.core.query.Criteria>();
        searchAttributes.forEach((k, v) ->
                criteriaList.add(org.springframework.data.mongodb.core.query.Criteria.where(k).is(v)));
        var query = org.springframework.data.mongodb.core.query.Query.query(
                new org.springframework.data.mongodb.core.query.Criteria().andOperator(
                        criteriaList.toArray(new org.springframework.data.mongodb.core.query.Criteria[0])));
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
