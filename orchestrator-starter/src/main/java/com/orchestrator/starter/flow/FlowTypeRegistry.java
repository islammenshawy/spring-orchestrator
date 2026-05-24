package com.orchestrator.starter.flow;

import java.util.*;

/**
 * Central registry of all flow types in the application.
 * Created at startup from discovered @Flow beans.
 *
 * Provides lookup by flowType name, entity class, or fallback to single flow.
 * The single-flow fallback ensures backward compatibility with apps that
 * have only one @Flow class and old Kafka messages without flowType field.
 */
public class FlowTypeRegistry {

    private final Map<String, FlowTypeDescriptor> byFlowType;
    private final Map<Class<?>, FlowTypeDescriptor> byEntityClass;
    private final Map<Class<?>, FlowTypeDescriptor> byFlowDefClass;

    public FlowTypeRegistry(Collection<FlowTypeDescriptor> descriptors) {
        this.byFlowType = new LinkedHashMap<>();
        this.byEntityClass = new LinkedHashMap<>();
        this.byFlowDefClass = new LinkedHashMap<>();
        for (FlowTypeDescriptor d : descriptors) {
            if (byFlowType.containsKey(d.getFlowType())) {
                throw new IllegalStateException(
                        "Duplicate flow type '" + d.getFlowType() + "' — " +
                        "each @Flow class must have a unique name. " +
                        "Use @Flow(name=\"...\") to set an explicit name.");
            }
            byFlowType.put(d.getFlowType(), d);
            byEntityClass.put(d.getEntityClass(), d);
            if (d.getFlowDefinitionClass() != null) {
                byFlowDefClass.put(d.getFlowDefinitionClass(), d);
            }
        }
    }

    /** Lookup by flowType name. Returns null if not found. */
    public FlowTypeDescriptor get(String flowType) {
        return byFlowType.get(flowType);
    }

    /** Lookup by entity class. Returns null if not found. */
    public FlowTypeDescriptor getByEntityClass(Class<?> entityClass) {
        return byEntityClass.get(entityClass);
    }

    /** Lookup by @Flow definition class. Returns null if not found. */
    public FlowTypeDescriptor getByFlowDefinitionClass(Class<? extends FlowDefinition> flowDefClass) {
        return byFlowDefClass.get(flowDefClass);
    }

    /** All registered flow types. */
    public Collection<FlowTypeDescriptor> getAll() {
        return Collections.unmodifiableCollection(byFlowType.values());
    }

    /** All flow type names. */
    public Set<String> getFlowTypeNames() {
        return Collections.unmodifiableSet(byFlowType.keySet());
    }

    /** Number of registered flow types. */
    public int size() {
        return byFlowType.size();
    }

    /**
     * Get the single registered flow type, or throw if there are multiple.
     * Used for backward compatibility when flowType is null in a Kafka message
     * (old messages from before multi-flow support).
     */
    public FlowTypeDescriptor getSingleOrThrow() {
        if (byFlowType.size() == 1) {
            return byFlowType.values().iterator().next();
        }
        throw new IllegalStateException(
                "Cannot determine flow type — message has no flowType field and " +
                byFlowType.size() + " flow types are registered: " + byFlowType.keySet() +
                ". Old messages without flowType only work with single-flow apps.");
    }

    /**
     * Resolve flowType from a message — uses explicit flowType if present,
     * falls back to single-flow for backward compatibility.
     */
    public FlowTypeDescriptor resolve(String flowType) {
        if (flowType != null && !flowType.isEmpty()) {
            FlowTypeDescriptor d = byFlowType.get(flowType);
            if (d == null) {
                throw new IllegalArgumentException(
                        "Unknown flow type '" + flowType + "'. Registered: " + byFlowType.keySet());
            }
            return d;
        }
        return getSingleOrThrow();
    }

    /** Get all unique command topics across all flow types. */
    public Set<String> getAllCommandTopics() {
        Set<String> topics = new LinkedHashSet<>();
        byFlowType.values().forEach(d -> topics.add(d.getCommandTopic()));
        return topics;
    }

    /** Get all unique reply topics across all flow types. */
    public Set<String> getAllReplyTopics() {
        Set<String> topics = new LinkedHashSet<>();
        byFlowType.values().stream()
                .filter(FlowTypeDescriptor::isReplyEnabled)
                .forEach(d -> topics.add(d.getReplyTopic()));
        return topics;
    }
}
