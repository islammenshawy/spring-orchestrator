package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Adapts a @Step-annotated method on a FlowDefinition class into
 * a StepHandler that the library can execute.
 *
 * Handles:
 * - Step name from annotation or method name (camelCase → UPPER_SNAKE)
 * - completedWhen SpEL evaluation for idempotency
 * - StepType for determining protection level
 * - Annotation inheritance: method-level overrides class-level
 */
@Slf4j
public class MethodStepAdapter<F extends OrchestratorFlow> implements StepHandler<F> {

    private static final ExpressionParser SPEL = new SpelExpressionParser();

    private final Object flowDefinition;
    private final Method method;
    private final Step stepAnnotation;
    private final String stepName;
    private Method compensateMethod; // null if no @Compensate defined

    public MethodStepAdapter(Object flowDefinition, Method method, Step stepAnnotation) {
        this.flowDefinition = flowDefinition;
        this.method = method;
        this.stepAnnotation = stepAnnotation;
        this.stepName = resolveStepName(method, stepAnnotation);
        this.method.setAccessible(true);
        this.compensateMethod = findCompensateMethod(flowDefinition.getClass(), method.getName());
    }

    @Override
    public String getStepName() {
        return stepName;
    }

    @Override
    public int getOrder() {
        return stepAnnotation.order();
    }

    @Override
    public boolean isAlreadyCompleted(F flow) {
        String expr = stepAnnotation.completedWhen();
        if (expr.isEmpty()) return false;

        try {
            StandardEvaluationContext ctx = new StandardEvaluationContext(flow);
            Boolean result = SPEL.parseExpression(expr).getValue(ctx, Boolean.class);
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            log.warn("[Step:{}] Failed to evaluate completedWhen='{}': {}", stepName, expr, e.getMessage());
            return false;
        }
    }

    @Override
    public void execute(F flow) {
        try {
            method.invoke(flowDefinition, flow);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException(cause);
        }
    }

    /**
     * Get annotation from method first, fall back to class level.
     */
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        A methodLevel = method.getAnnotation(annotationType);
        if (methodLevel != null) return methodLevel;
        return flowDefinition.getClass().getAnnotation(annotationType);
    }

    public RetryOn getRetryOn() { return getAnnotation(RetryOn.class); }
    public FailOn getFailOn() { return getAnnotation(FailOn.class); }
    public RecoverOn[] getRecoverOns() {
        RecoverOn[] methodLevel = method.getAnnotationsByType(RecoverOn.class);
        if (methodLevel.length > 0) return methodLevel;
        return flowDefinition.getClass().getAnnotationsByType(RecoverOn.class);
    }

    public StepType getStepType() {
        return stepAnnotation.type();
    }

    public boolean hasCompensation() {
        return compensateMethod != null;
    }

    public void compensate(F flow) {
        if (compensateMethod == null) {
            log.warn("[Step:{}] No @Compensate method defined, skipping compensation", stepName);
            return;
        }
        try {
            log.info("[Step:{}] Executing compensation", stepName);
            compensateMethod.invoke(flowDefinition, flow);
        } catch (Exception e) {
            log.error("[Step:{}] Compensation failed: {}", stepName, e.getMessage());
        }
    }

    private static Method findCompensateMethod(Class<?> clazz, String stepMethodName) {
        for (Method m : clazz.getDeclaredMethods()) {
            Compensate comp = m.getAnnotation(Compensate.class);
            if (comp != null && comp.step().equals(stepMethodName)) {
                m.setAccessible(true);
                return m;
            }
        }
        return null;
    }

    private static String resolveStepName(Method method, Step annotation) {
        if (!annotation.name().isEmpty()) return annotation.name();
        // Convert camelCase method name to UPPER_SNAKE_CASE
        return method.getName()
                .replaceAll("([a-z])([A-Z])", "$1_$2")
                .toUpperCase();
    }
}
