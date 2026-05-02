package com.orchestrator.starter.flow;

import com.orchestrator.starter.audit.StepExecutionLog;
import com.orchestrator.starter.audit.StepExecutionLogRepository;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import com.orchestrator.starter.kafka.StepCommandMessage;
import com.orchestrator.starter.kafka.StepReplyMessage;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
    private Class<F> entityClass;
    private org.springframework.data.mongodb.core.MongoTemplate mongoTemplate;

    public FlowOrchestrator(
            OrchestratorFlowRepository<F> flowRepository,
            StepRegistry<F> stepRegistry,
            OutboxEventRepository outboxRepository,
            StepExecutionLogRepository stepLogRepository,
            ObjectMapper objectMapper,
            String commandTopic,
            String replyTopic,
            boolean replyEnabled,
            TransactionTemplate txTemplate,
            boolean includeFlowStateInLogs,
            KafkaTemplate kafkaTemplate) {
        this(flowRepository, stepRegistry, outboxRepository, stepLogRepository,
                objectMapper, null, commandTopic, replyTopic, replyEnabled,
                txTemplate, includeFlowStateInLogs, kafkaTemplate);
    }

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
            KafkaTemplate kafkaTemplate) {
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

        // Publish to Kafka synchronously — message is in Kafka before we return
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(savedFlow.getId())
                    .correlationId(savedFlow.getCorrelationId())
                    .stepName(savedFlow.getCurrentStep())
                    .flowType(flowType)
                    .build();
            kafkaTemplate.send(commandTopic, savedFlow.getId(),
                    objectMapper.writeValueAsString(cmd)).get();
        } catch (Exception e) {
            // Kafka publish failed — outbox publisher will pick it up (~500ms)
            log.warn("[Saga] Direct Kafka publish failed for flow {}, outbox will retry: {}",
                    savedFlow.getId(), e.getMessage());
        }

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
        F flow = flowRepository.findById(flowId)
                .orElseThrow(() -> new IllegalArgumentException("Flow not found: " + flowId));

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
                    .allMatch(ps -> ps.isAlreadyCompleted(flow));
            if (!allDone) {
                log.info("[Saga] Join {} waiting — not all parallel steps in group '{}' completed",
                        stepName, group);
                return; // Don't ack — message will be redelivered
            }
            log.info("[Saga] Join {} — all parallel steps in group '{}' completed, proceeding",
                    stepName, group);
        }

        // Layer 2 idempotency
        if (handler.isAlreadyCompleted(flow)) {
            log.info("[Saga] Step {} already completed for flow {}, marking parallel + advancing",
                    stepName, flowId);
            markParallelStepCompleted(flow, stepName, handler);
            return;
        }

        flow.setStatus(FlowStatus.IN_PROGRESS);
        String flowBefore = includeFlowStateInLogs ? serialize(flow) : null;
        Instant startedAt = Instant.now();

        log.info("[Saga] Executing step {} for flow {}", stepName, flowId);

        try {
            handler.execute(flow);
        } catch (WaitingStepException e) {
            // Gate/polling step — fixed short delay with jitter, bypass exponential retry topics
            logStep(flowId, stepName, "WAITING", flow.getRetryCount(),
                    flowBefore, null, e.getMessage(), startedAt);
            handleWaitingStep(flow, e);
            return; // Do NOT throw — we re-publish directly, bypassing Spring Kafka retry
        } catch (RetryableStepException e) {
            logStep(flowId, stepName, "RETRYING", flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handleRetryableFailure(flow, e);
            throw e;
        } catch (NonRetryableStepException e) {
            logStep(flowId, stepName, "FAILED", flow.getRetryCount() + 1,
                    flowBefore, null, e.getMessage(), startedAt);
            handlePermanentFailure(flow, e);
            return;
        } catch (Exception e) {
            try {
                StepErrorHandler.handleError(handler, e);
                // Recovered (e.g., HTTP 409)
                logStep(flowId, stepName, "RECOVERED", flow.getRetryCount() + 1,
                        flowBefore, includeFlowStateInLogs ? serialize(flow) : null, e.getMessage(), startedAt);
                log.info("[Saga] Step {} recovered for flow {}", stepName, flowId);
            } catch (RetryableStepException re) {
                logStep(flowId, stepName, "RETRYING", flow.getRetryCount() + 1,
                        flowBefore, null, re.getMessage(), startedAt);
                handleRetryableFailure(flow, re);
                throw re;
            } catch (NonRetryableStepException nre) {
                logStep(flowId, stepName, "FAILED", flow.getRetryCount() + 1,
                        flowBefore, null, nre.getMessage(), startedAt);
                handlePermanentFailure(flow, nre);
                return;
            }
        }

        // Step succeeded — clear retry state via $set (no @Version conflict).
        // Domain fields are already persisted by checkpoint() in the step handler.
        flow.setRetryCount(0);
        flow.setBackoffSeconds(0);
        flow.setNextRetryAt(null);
        flow.setErrorMessage(null);
        flow.setUpdatedAt(Instant.now());

        // Save flow with version conflict retry.
        // The reply consumer may have incremented the version via $set.
        // On conflict: re-read latest version, copy it to our flow object, retry save.
        // This preserves all domain fields set by the step handler.
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                saveFlow(flow);
                break;
            } catch (org.springframework.dao.OptimisticLockingFailureException e) {
                log.debug("[Saga] Version conflict saving flow {} (attempt {}), retrying", flowId, attempt + 1);
                F fresh = flowRepository.findById(flowId).orElse(null);
                if (fresh instanceof com.orchestrator.starter.domain.AbstractFlow af
                        && flow instanceof com.orchestrator.starter.domain.AbstractFlow afFlow) {
                    afFlow.setVersion(af.getVersion());
                    afFlow.setCurrentStep(af.getCurrentStep()); // reply may have advanced
                }
            }
        }

        if (replyEnabled) {
            publishReply(flowId, stepName, "COMPLETED", null, serialize(flow));
        } else {
            // Inline mode: advance in same thread
            markParallelStepCompleted(flow, stepName, handler);
        }

        logStep(flowId, stepName, "COMPLETED", 1,
                flowBefore, includeFlowStateInLogs ? serialize(flow) : null, null, startedAt);
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
     * Advances the flow to the next step.
     */
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
        if (flow.getStatus() == FlowStatus.COMPLETED || flow.getStatus() == FlowStatus.FAILED) return;

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
            kafkaTemplate.send(replyTopic, flowId, objectMapper.writeValueAsString(reply)).get();
        } catch (Exception e) {
            log.warn("[Saga] Reply publish failed for flow {} step {}: {}",
                    flowId, stepName, e.getMessage());
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
            logStep(flowId, stepName != null ? stepName : "UNKNOWN", "DEAD_LETTERED", 0,
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
        saveFlow(flow);

        logStep(flowId, stepName != null ? stepName : flow.getCurrentStep(),
                "DEAD_LETTERED", flow.getRetryCount(),
                null, null, errorDetail, Instant.now());

        // Run compensation for all completed steps in reverse
        runCompensation(flow);
    }

    // ========== Cancellation ==========

    /**
     * Cancel a running flow. Runs @OnCancel handlers (or @Compensate fallback)
     * for all completed steps in reverse order, then marks as CANCELLED.
     *
     * Can only cancel flows in IN_PROGRESS, WAITING_RETRY, or PENDING status.
     * Returns the cancelled flow, or null if cancellation not allowed.
     */
    public F cancelFlow(String flowId, String reason) {
        F flow = flowRepository.findById(flowId).orElse(null);
        if (flow == null) return null;

        FlowStatus status = flow.getStatus();
        if (status != FlowStatus.IN_PROGRESS && status != FlowStatus.WAITING_RETRY
                && status != FlowStatus.PENDING) {
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
        // Include current step if it has a result (completedWhen is true)
        String currentStep = flow.getCurrentStep();
        if (currentStep != null) {
            StepHandler<F> currentHandler = stepRegistry.getHandler(currentStep);
            if (currentHandler != null && currentHandler.isAlreadyCompleted(flow)) {
                completedSteps = new ArrayList<>(completedSteps);
                completedSteps.add(currentStep);
            }
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
                    logStep(flow.getId(), stepName, "CANCELLED", 1, null, null, null, start);
                } catch (Exception e) {
                    log.error("[Saga] Cancel handler failed for step {} on flow {}: {}",
                            stepName, flow.getId(), e.getMessage());
                    logStep(flow.getId(), stepName, "CANCEL_FAILED", 1,
                            null, null, e.getMessage(), start);
                }
            }
        }

        flow.setStatus(FlowStatus.CANCELLED);
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);
        log.info("[Saga] Flow {} cancelled", flow.getId());
    }

    // ========== Compensation (what makes this a Saga) ==========

    private void runCompensation(F flow) {
        List<String> completedSteps = stepRegistry.getCompletedStepsBefore(flow.getCurrentStep());
        if (completedSteps.isEmpty()) return;

        log.info("[Saga] Running compensation for flow {} — {} steps to undo",
                flow.getId(), completedSteps.size());

        flow.setStatus(FlowStatus.COMPENSATING);
        saveFlow(flow);

        // Compensate in reverse order
        for (int i = completedSteps.size() - 1; i >= 0; i--) {
            String stepName = completedSteps.get(i);
            StepHandler<F> handler = stepRegistry.getHandler(stepName);

            if (handler instanceof MethodStepAdapter<F> adapter && adapter.hasCompensation()) {
                Instant start = Instant.now();
                try {
                    adapter.compensate(flow);
                    logStep(flow.getId(), stepName, "COMPENSATED", 1, null, null, null, start);
                } catch (Exception e) {
                    log.error("[Saga] Compensation failed for step {} on flow {}: {}",
                            stepName, flow.getId(), e.getMessage());
                    logStep(flow.getId(), stepName, "COMPENSATION_FAILED", 1,
                            null, null, e.getMessage(), start);
                }
            } else {
                log.warn("[Saga] No @Compensate for step {}, skipping", stepName);
            }
        }

        flow.setStatus(FlowStatus.FAILED);
        flow.setUpdatedAt(Instant.now());
        saveFlow(flow);
    }

    // ========== Internal ==========

    private void advanceToNextStep(F flow) {
        advanceToNextStep(flow, flow.getCurrentStep());
    }

    private void advanceToNextStep(F flow, String completedStep) {
        String nextStep = stepRegistry.getNextStep(completedStep);

        if (nextStep == null) {
            // Flow complete — use partial $set to avoid @Version conflicts.
            // Only updates status/updatedAt, preserves all domain fields.
            updateFlowPartial(flow.getId(), java.util.Map.of(
                    "status", FlowStatus.COMPLETED.name(),
                    "updatedAt", Instant.now(),
                    "completedParallelSteps", java.util.List.of()));
            log.info("[Saga] Flow {} completed", flow.getId());
            return;
        }

        // Advance to next step — partial update + outbox event.
        // The partial update avoids @Version conflicts with the command consumer.
        updateFlowPartial(flow.getId(), java.util.Map.of(
                "currentStep", nextStep,
                "updatedAt", Instant.now()));

        // Write outbox event for the next step command.
        // Re-read the flow to get the latest state for serialization.
        F latest = flowRepository.findById(flow.getId()).orElse(flow);
        latest.setCurrentStep(nextStep);

        List<String> stepsAtNextOrder = stepRegistry.getStepsAtSameOrder(nextStep);
        if (stepsAtNextOrder.size() > 1) {
            for (String parallelStep : stepsAtNextOrder) {
                writeOutboxEvent(latest, parallelStep);
            }
            log.info("[Saga] Published {} parallel steps for flow {}: {}",
                    stepsAtNextOrder.size(), latest.getId(), stepsAtNextOrder);
        } else {
            writeOutboxEvent(latest);
        }
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
     * Gate/polling step — re-publish to main command topic with a short fixed delay + jitter.
     * Bypasses Spring Kafka's exponential retry topics entirely.
     * Does NOT increment retryCount (this isn't an error, just waiting).
     */
    private void handleWaitingStep(F flow, WaitingStepException e) {
        String errorMsg = e.getMessage() != null ? e.getMessage() : "waiting for external event";
        Instant nextRetry = Instant.now().plusMillis(e.getDelayMs());

        if (mongoTemplate != null && entityClass != null) {
            updateFlowPartial(flow.getId(), java.util.Map.of(
                    "status", FlowStatus.WAITING_RETRY.name(),
                    "nextRetryAt", nextRetry,
                    "errorMessage", errorMsg,
                    "updatedAt", Instant.now()));
        } else {
            flow.setStatus(FlowStatus.WAITING_RETRY);
            flow.setNextRetryAt(nextRetry);
            flow.setErrorMessage(errorMsg);
            flow.setUpdatedAt(Instant.now());
            saveFlow(flow);
        }

        // Schedule direct re-publish to main command topic after the delay
        Thread.startVirtualThread(() -> {
            try {
                Thread.sleep(e.getDelayMs());
                publishStepCommand(flow.getId(), flow.getCurrentStep());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                log.warn("[Saga] Failed to re-publish waiting step {} for flow {}: {}",
                        flow.getCurrentStep(), flow.getId(), ex.getMessage());
            }
        });
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
            boolean allDone = siblings.stream().allMatch(s -> s.isAlreadyCompleted(flow));

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

    /** Direct publish to main command topic — used by waiting steps to bypass retry topics. */
    @SuppressWarnings("unchecked")
    private void publishStepCommand(String flowId, String stepName) {
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .correlationId(flowId)
                    .stepName(stepName)
                    .flowType(flowType)
                    .build();
            kafkaTemplate.send(commandTopic, flowId, objectMapper.writeValueAsString(cmd));
        } catch (Exception e) {
            log.warn("[Saga] Failed to publish waiting step command for {}: {}", flowId, e.getMessage());
        }
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

            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flow.getId())
                    .topic(commandTopic)
                    .key(flow.getId())
                    .payload(objectMapper.writeValueAsString(cmd))
                    .build();

            outboxRepository.save(event);
        } catch (Exception e) {
            log.error("[Saga] Failed to write outbox event: {}", e.getMessage());
        }
    }

    private void logStep(String flowId, String stepName, String status, int attempt,
                         String before, String after, String error, Instant startedAt) {
        try {
            Instant now = Instant.now();
            stepLogRepository.save(StepExecutionLog.builder()
                    .id(UUID.randomUUID().toString())
                    .flowId(flowId)
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

    public void setEntityClass(Class<F> entityClass) {
        this.entityClass = entityClass;
    }

    public void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
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

    private String serialize(F flow) {
        try {
            return objectMapper.writeValueAsString(flow);
        } catch (Exception e) {
            return "{}";
        }
    }
}
