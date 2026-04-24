package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Scans the Spring context for @Flow classes, discovers their @Step methods,
 * and creates MethodStepAdapter instances that the StepRegistry can use.
 *
 * Called during auto-configuration to bridge the single-class flow definition
 * to the library's StepHandler-based engine.
 */
@Slf4j
public class FlowDefinitionScanner {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static List<StepHandler> scan(ApplicationContext context) {
        List<StepHandler> handlers = new ArrayList<>();

        // Find all beans annotated with @Flow
        Map<String, Object> flowBeans = context.getBeansWithAnnotation(Flow.class);

        for (Map.Entry<String, Object> entry : flowBeans.entrySet()) {
            Object flowDef = entry.getValue();
            Class<?> clazz = flowDef.getClass();

            // Scan methods for @Step
            for (Method method : clazz.getDeclaredMethods()) {
                Step stepAnnotation = method.getAnnotation(Step.class);
                if (stepAnnotation == null) continue;

                // Validate method signature: must accept exactly one OrchestratorFlow parameter
                if (method.getParameterCount() != 1 ||
                        !OrchestratorFlow.class.isAssignableFrom(method.getParameterTypes()[0])) {
                    throw new IllegalStateException(
                            "@Step method " + clazz.getSimpleName() + "." + method.getName() +
                                    " must accept exactly one parameter extending OrchestratorFlow");
                }

                MethodStepAdapter adapter = new MethodStepAdapter(flowDef, method, stepAnnotation);
                handlers.add(adapter);
                log.debug("Registered step: {} (order={}, type={}) from {}",
                        adapter.getStepName(), adapter.getOrder(),
                        stepAnnotation.type(), clazz.getSimpleName());
            }

            if (handlers.isEmpty()) {
                log.warn("@Flow class {} has no @Step methods", clazz.getSimpleName());
            } else {
                handlers.sort(Comparator.comparingInt(StepHandler::getOrder));
                log.info("Discovered @Flow {} with {} steps: {}",
                        clazz.getSimpleName(), handlers.size(),
                        handlers.stream().map(StepHandler::getStepName).toList());
            }
        }

        return handlers;
    }
}
