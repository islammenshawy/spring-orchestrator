package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.Compensate;
import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

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
                if (!stepOrders.add(stepAnnotation.order())) {
                    throw new IllegalStateException(
                            "@Flow " + clazz.getSimpleName() + ": duplicate step order " +
                                    stepAnnotation.order() + " on method " + method.getName());
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
}
