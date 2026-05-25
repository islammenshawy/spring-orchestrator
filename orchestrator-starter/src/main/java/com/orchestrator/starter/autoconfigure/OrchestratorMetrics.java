package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.flow.FlowOrchestrator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Centralized metrics for the orchestrator library.
 * Null-safe: all methods are no-ops when MeterRegistry is absent.
 * Callers never need null checks — just call directly.
 */
public class OrchestratorMetrics {

    private static final OrchestratorMetrics NOOP = new OrchestratorMetrics(null);

    private final MeterRegistry registry;

    public OrchestratorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Returns a no-op instance. All method calls are silently ignored. */
    public static OrchestratorMetrics noop() {
        return NOOP;
    }

    public boolean isEnabled() {
        return registry != null;
    }

    // ========== Flow lifecycle ==========

    public void flowStarted(String flowType) {
        if (registry == null) return;
        counter("orchestrator.flows.started", flowType);
    }

    public void flowCompleted(String flowType) {
        if (registry == null) return;
        counter("orchestrator.flows.completed", flowType);
    }

    public void flowFailed(String flowType) {
        if (registry == null) return;
        counter("orchestrator.flows.failed", flowType);
    }

    public void compensationFailed(String flowType) {
        if (registry == null) return;
        counter("orchestrator.compensation.failed", flowType);
    }

    // ========== Step execution ==========

    public void stepExecution(String flowType, String stepName, String outcome, Duration duration) {
        if (registry == null) return;
        Timer.builder("orchestrator.step.executions")
                .tag("flowType", safe(flowType))
                .tag("stepName", stepName)
                .tag("outcome", outcome)
                .register(registry).record(duration);
    }

    // ========== Outbox ==========

    public void outboxPublished() {
        if (registry == null) return;
        Counter.builder("orchestrator.outbox.published").register(registry).increment();
    }

    public void outboxDeadLettered() {
        if (registry == null) return;
        Counter.builder("orchestrator.outbox.dead_lettered").register(registry).increment();
    }

    // ========== Recovery ==========

    public void recoveryRecovered(String flowType) {
        if (registry == null) return;
        counter("orchestrator.recovery.recovered", flowType);
    }

    // ========== Idempotency ==========

    public void idempotencyDuplicate() {
        if (registry == null) return;
        Counter.builder("orchestrator.idempotency.duplicates").register(registry).increment();
    }

    // ========== Internal ==========

    private void counter(String name, String flowType) {
        Counter.builder(name).tag("flowType", safe(flowType)).register(registry).increment();
    }

    private static String safe(String flowType) {
        return flowType != null ? flowType : FlowOrchestrator.DEFAULT_FLOW_TYPE;
    }
}
