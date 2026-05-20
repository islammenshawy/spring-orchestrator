package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.Compensate;
import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.JoinOn;
import com.orchestrator.starter.annotation.Parallel;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.SpelParseException;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Scans @Flow classes at startup, discovers @Step and @Compensate methods,
 * validates wiring, and fails fast on misconfiguration.
 *
 * Validates:
 * - @Step methods have exactly one OrchestratorFlow parameter
 * - @Compensate(step=X) references an existing @Step method name
 * - @Compensate methods have exactly one OrchestratorFlow parameter
 * - No duplicate step orders
 * - No duplicate step names
 */
@Slf4j
public class FlowDefinitionScanner {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static List<StepHandler> scan(ApplicationContext context) {
        List<StepHandler> handlers = new ArrayList<>();

        Map<String, Object> flowBeans = context.getBeansWithAnnotation(Flow.class);

        for (Map.Entry<String, Object> entry : flowBeans.entrySet()) {
            Object flowDef = entry.getValue();
            Class<?> clazz = flowDef.getClass();

            // Collect @Step method names for validation
            Set<String> stepMethodNames = new HashSet<>();
            Set<Integer> stepOrders = new HashSet<>();
            Set<String> stepNames = new HashSet<>();

            // Pass 1: discover @Step methods
            for (Method method : clazz.getDeclaredMethods()) {
                Step stepAnnotation = method.getAnnotation(Step.class);
                if (stepAnnotation == null) continue;

                // Validate signature
                if (method.getParameterCount() != 1 ||
                        !OrchestratorFlow.class.isAssignableFrom(method.getParameterTypes()[0])) {
                    throw new IllegalStateException(
                            "@Step method " + clazz.getSimpleName() + "." + method.getName() +
                                    " must accept exactly one parameter extending OrchestratorFlow");
                }

                // Validate no duplicate order
                // Duplicate orders allowed for @Parallel steps (same order = parallel execution)
                if (!stepOrders.add(stepAnnotation.order())
                        && !method.isAnnotationPresent(Parallel.class)) {
                    throw new IllegalStateException(
                            "@Flow " + clazz.getSimpleName() + ": duplicate step order " +
                                    stepAnnotation.order() + " on method " + method.getName() +
                                    ". Use @Parallel for concurrent steps.");
                }

                // Validate no duplicate name
                String resolvedName = stepAnnotation.name().isEmpty()
                        ? method.getName().replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase()
                        : stepAnnotation.name();
                if (!stepNames.add(resolvedName)) {
                    throw new IllegalStateException(
                            "@Flow " + clazz.getSimpleName() + ": duplicate step name '" +
                                    resolvedName + "'");
                }

                stepMethodNames.add(method.getName());

                MethodStepAdapter adapter = new MethodStepAdapter(flowDef, method, stepAnnotation);
                handlers.add(adapter);
            }

            // Pass 2: validate @Compensate references
            for (Method method : clazz.getDeclaredMethods()) {
                Compensate comp = method.getAnnotation(Compensate.class);
                if (comp == null) continue;

                // Validate: @Compensate(step=X) must reference an existing @Step method
                if (!stepMethodNames.contains(comp.step())) {
                    throw new IllegalStateException(
                            "@Compensate on " + clazz.getSimpleName() + "." + method.getName() +
                                    " references step '" + comp.step() +
                                    "' but no @Step method with that name exists. " +
                                    "Available steps: " + stepMethodNames);
                }

                // Validate signature
                if (method.getParameterCount() != 1 ||
                        !OrchestratorFlow.class.isAssignableFrom(method.getParameterTypes()[0])) {
                    throw new IllegalStateException(
                            "@Compensate method " + clazz.getSimpleName() + "." + method.getName() +
                                    " must accept exactly one parameter extending OrchestratorFlow");
                }
            }

            // Pass 3: validate @Parallel and @JoinOn
            Set<String> parallelGroups = new HashSet<>();
            Set<String> joinGroups = new HashSet<>();
            for (Method method : clazz.getDeclaredMethods()) {
                Parallel parallel = method.getAnnotation(Parallel.class);
                if (parallel != null) {
                    parallelGroups.add(parallel.group());
                    // Parallel steps must have completedWhen (needed for join check)
                    Step step = method.getAnnotation(Step.class);
                    if (step != null && step.completedWhen().isEmpty()) {
                        throw new IllegalStateException(
                                "@Parallel step " + clazz.getSimpleName() + "." + method.getName() +
                                        " must have completedWhen (needed for join verification)");
                    }
                }
                JoinOn joinOn = method.getAnnotation(JoinOn.class);
                if (joinOn != null) joinGroups.add(joinOn.group());
            }
            // Every @JoinOn group must have matching @Parallel group
            for (String group : joinGroups) {
                if (!parallelGroups.contains(group)) {
                    throw new IllegalStateException(
                            "@JoinOn references group '" + group + "' but no @Parallel steps " +
                                    "with that group exist in " + clazz.getSimpleName());
                }
            }

            if (handlers.isEmpty()) {
                log.warn("@Flow class {} has no @Step methods", clazz.getSimpleName());
            } else {
                handlers.sort(Comparator.comparingInt(StepHandler::getOrder));
                log.info("Discovered @Flow {} with {} steps: {}",
                        clazz.getSimpleName(), handlers.size(),
                        handlers.stream().map(StepHandler::getStepName)
                                .collect(Collectors.toList()));
            }
        }

        return handlers;
    }

    /**
     * Scans @Flow beans and returns step handlers grouped by flow type.
     * Each key is a flowType name, each value is the sorted list of handlers for that flow.
     *
     * Also returns flow metadata (entity class, topic annotation) via FlowTypeInfo.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Map<String, FlowTypeInfo> scanByFlowType(ApplicationContext context) {
        Map<String, FlowTypeInfo> result = new LinkedHashMap<>();

        Map<String, Object> flowBeans = context.getBeansWithAnnotation(Flow.class);

        for (Map.Entry<String, Object> entry : flowBeans.entrySet()) {
            Object flowDef = entry.getValue();
            Class<?> clazz = flowDef.getClass();
            Flow flowAnn = clazz.getAnnotation(Flow.class);
            if (flowAnn == null) continue;

            // Derive flowType name
            String flowType = flowAnn.name().isEmpty()
                    ? deriveFlowType(clazz)
                    : flowAnn.name();

            // Derive entity class from FlowDefinition<F>
            Class<?> entityClass = discoverEntityClass(clazz);

            // Scan steps (reuse existing validation logic)
            List<StepHandler> handlers = new ArrayList<>();
            Set<String> stepMethodNames = new HashSet<>();
            Set<Integer> stepOrders = new HashSet<>();
            Set<String> stepNames = new HashSet<>();

            for (Method method : clazz.getDeclaredMethods()) {
                Step stepAnnotation = method.getAnnotation(Step.class);
                if (stepAnnotation == null) continue;

                if (method.getParameterCount() != 1 ||
                        !OrchestratorFlow.class.isAssignableFrom(method.getParameterTypes()[0])) {
                    throw new IllegalStateException(
                            "@Step method " + clazz.getSimpleName() + "." + method.getName() +
                                    " must accept exactly one parameter extending OrchestratorFlow");
                }
                // Duplicate orders allowed for @Parallel steps (same order = parallel execution)
                if (!stepOrders.add(stepAnnotation.order())
                        && !method.isAnnotationPresent(Parallel.class)) {
                    throw new IllegalStateException(
                            "@Flow " + clazz.getSimpleName() + ": duplicate step order " +
                                    stepAnnotation.order() + " on method " + method.getName() +
                                    ". Use @Parallel for concurrent steps.");
                }
                String resolvedName = stepAnnotation.name().isEmpty()
                        ? method.getName().replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase()
                        : stepAnnotation.name();
                if (!stepNames.add(resolvedName)) {
                    throw new IllegalStateException(
                            "@Flow " + clazz.getSimpleName() + ": duplicate step name '" + resolvedName + "'");
                }
                // Validate SpEL completedWhen at startup (catch parse errors early)
                if (!stepAnnotation.completedWhen().isEmpty()) {
                    try {
                        new SpelExpressionParser().parseExpression(stepAnnotation.completedWhen());
                    } catch (SpelParseException e) {
                        throw new IllegalStateException(
                                "Invalid SpEL in @Step(completedWhen=\"" + stepAnnotation.completedWhen() +
                                        "\") on " + clazz.getSimpleName() + "." + method.getName() +
                                        ": " + e.getMessage());
                    }
                }

                // Validate expiresAfter format at startup
                if (!stepAnnotation.expiresAfter().isEmpty()) {
                    try {
                        MethodStepAdapter.parseExpiresAfter(stepAnnotation.expiresAfter());
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                "Invalid @Step(expiresAfter=\"" + stepAnnotation.expiresAfter() +
                                        "\") on " + clazz.getSimpleName() + "." + method.getName() +
                                        " — use format like '48h' or '7d'");
                    }
                }

                stepMethodNames.add(method.getName());
                handlers.add(new MethodStepAdapter(flowDef, method, stepAnnotation));
            }

            // Validate @Compensate and @Parallel/@JoinOn (same as scan())
            Set<String> parallelGroups = new HashSet<>();
            Set<String> joinGroups = new HashSet<>();
            for (Method method : clazz.getDeclaredMethods()) {
                Compensate comp = method.getAnnotation(Compensate.class);
                if (comp != null) {
                    if (!stepMethodNames.contains(comp.step())) {
                        throw new IllegalStateException(
                                "@Compensate on " + clazz.getSimpleName() + "." + method.getName() +
                                        " references step '" + comp.step() + "' — available: " + stepMethodNames);
                    }
                    if (method.getParameterCount() != 1 ||
                            !OrchestratorFlow.class.isAssignableFrom(method.getParameterTypes()[0])) {
                        throw new IllegalStateException(
                                "@Compensate method " + clazz.getSimpleName() + "." + method.getName() +
                                        " must accept exactly one OrchestratorFlow parameter");
                    }
                }
                Parallel parallel = method.getAnnotation(Parallel.class);
                if (parallel != null) {
                    parallelGroups.add(parallel.group());
                    Step step = method.getAnnotation(Step.class);
                    if (step != null && step.completedWhen().isEmpty()) {
                        throw new IllegalStateException("@Parallel step must have completedWhen");
                    }
                }
                JoinOn joinOn = method.getAnnotation(JoinOn.class);
                if (joinOn != null) joinGroups.add(joinOn.group());
            }
            for (String group : joinGroups) {
                if (!parallelGroups.contains(group)) {
                    throw new IllegalStateException(
                            "@JoinOn references group '" + group + "' — no @Parallel with that group in " +
                                    clazz.getSimpleName());
                }
            }

            handlers.sort(Comparator.comparingInt(StepHandler::getOrder));

            log.info("Discovered @Flow '{}' ({}) with {} steps: {}",
                    flowType, clazz.getSimpleName(), handlers.size(),
                    handlers.stream().map(StepHandler::getStepName).collect(Collectors.toList()));

            result.put(flowType, new FlowTypeInfo(flowType, clazz, entityClass,
                    flowAnn.topic(), handlers));
        }

        return result;
    }

    /** Derive flowType from class name: EnigioDocumentFlow → enigio-document */
    static String deriveFlowType(Class<?> clazz) {
        String name = clazz.getSimpleName();
        // Remove "Flow" suffix
        if (name.endsWith("Flow")) name = name.substring(0, name.length() - 4);
        // CamelCase → kebab-case
        return name.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase();
    }

    /** Discover entity class from FlowDefinition<F> generic parameter. */
    private static Class<?> discoverEntityClass(Class<?> clazz) {
        java.lang.reflect.Type superclass = clazz.getGenericSuperclass();
        if (superclass instanceof java.lang.reflect.ParameterizedType pt) {
            java.lang.reflect.Type[] args = pt.getActualTypeArguments();
            if (args.length > 0 && args[0] instanceof Class<?> entityClass) {
                return entityClass;
            }
        }
        return com.orchestrator.starter.domain.AbstractFlow.class;
    }

    /**
     * Metadata about a discovered flow type.
     */
    @SuppressWarnings("rawtypes")
    public record FlowTypeInfo(
            String flowType,
            Class<?> flowDefinitionClass,
            Class<?> entityClass,
            String annotatedTopic,
            List<StepHandler> handlers
    ) {}
}
