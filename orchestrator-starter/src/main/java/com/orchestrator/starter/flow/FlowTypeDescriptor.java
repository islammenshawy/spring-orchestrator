package com.orchestrator.starter.flow;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import lombok.Builder;
import lombok.Data;

/**
 * Holds the resolved configuration and runtime beans for a single flow type.
 * One instance per @Flow class in the application.
 *
 * Created at startup by scanning @Flow beans and merging with YAML config.
 */
@Data
@Builder
public class FlowTypeDescriptor {

    /** Flow type name — from @Flow(name="...") or derived from class name. */
    private final String flowType;

    /** The entity class (e.g., EnigioFlow.class) — discovered via FlowDefinition<F> generics. */
    private final Class<?> entityClass;

    /** The @Flow-annotated bean class. */
    private final Class<?> flowDefinitionClass;

    /** Resolved Kafka command topic (per-flow override or global default). */
    private final String commandTopic;

    /** Resolved reply topic. */
    private final String replyTopic;

    /** Resolved DLT topic (per-flow override or standard suffix). */
    private final String dltTopic;

    /** Whether reply mode is enabled. */
    private final boolean replyEnabled;

    /** Per-flow step registry. */
    private StepRegistry<?> stepRegistry;

    /** Per-flow repository. */
    private OrchestratorFlowRepository<?> repository;

    /** Per-flow orchestrator. */
    private FlowOrchestrator<?> orchestrator;

    /** Resolved retry config (per-flow override or global default). */
    private final OrchestratorProperties.RetryConfig retryConfig;
}
