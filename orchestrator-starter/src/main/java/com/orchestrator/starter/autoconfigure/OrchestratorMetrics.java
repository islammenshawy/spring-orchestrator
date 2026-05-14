package com.orchestrator.starter.autoconfigure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Centralized metrics for the orchestrator library.
 * All metric names and tags are defined here — no string literals scattered across classes.
 *
 * When MeterRegistry is null (actuator not on classpath), all methods are no-ops.
 */
public class OrchestratorMetrics {

    private final MeterRegistry registry;

    public OrchestratorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void flowStarted(String flowType) {
        if (registry == null) return;
        Counter.builder("orchestrator.flows.started")
                .tag("flowType", flowType)
                .register(registry).increment();
    }

    public void flowCompleted(String flowType) {
        if (registry == null) return;
        Counter.builder("orchestrator.flows.completed")
                .tag("flowType", flowType)
                .register(registry).increment();
    }

    public void flowFailed(String flowType) {
        if (registry == null) return;
        Counter.builder("orchestrator.flows.failed")
                .tag("flowType", flowType)
                .register(registry).increment();
    }

    public void stepExecution(String flowType, String stepName, String outcome, Duration duration) {
        if (registry == null) return;
        Timer.builder("orchestrator.step.executions")
                .tag("flowType", flowType)
                .tag("stepName", stepName)
                .tag("outcome", outcome)
                .register(registry).record(duration);
    }

    public void outboxPublished() {
        if (registry == null) return;
        Counter.builder("orchestrator.outbox.published")
                .register(registry).increment();
    }

    public void outboxDeadLettered() {
        if (registry == null) return;
        Counter.builder("orchestrator.outbox.dead_lettered")
                .register(registry).increment();
    }

    public void recoveryRecovered(String flowType) {
        if (registry == null) return;
        Counter.builder("orchestrator.recovery.recovered")
                .tag("flowType", flowType)
                .register(registry).increment();
    }

    public void idempotencyDuplicate() {
        if (registry == null) return;
        Counter.builder("orchestrator.idempotency.duplicates")
                .register(registry).increment();
    }
}
