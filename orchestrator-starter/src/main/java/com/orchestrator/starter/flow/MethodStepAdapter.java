package com.orchestrator.starter.flow;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Adapts a @Step-annotated method on a FlowDefinition class into
 * a StepHandler that the library can execute.
 *
 * Handles:
 * - Step name from annotation or method name (camelCase → UPPER_SNAKE)
 * - Annotation inheritance: method-level overrides class-level
 */
@Slf4j
public class MethodStepAdapter<F extends OrchestratorFlow> implements StepHandler<F> {

    private final Object flowDefinition;
    private final Method method;
    private final Step stepAnnotation;
    private final String stepName;
    private final String parallelGroup;  // null if not parallel
    private final String joinOnGroup;    // null if not a join point
    private Method compensateMethod;
    private Method cancelMethod;

    public MethodStepAdapter(Object flowDefinition, Method method, Step stepAnnotation) {
        this.flowDefinition = flowDefinition;
        this.method = method;
        this.stepAnnotation = stepAnnotation;
        this.stepName = resolveStepName(method, stepAnnotation);
        this.method.setAccessible(true);

        Parallel parallel = method.getAnnotation(Parallel.class);
        this.parallelGroup = parallel != null ? parallel.group() : null;

        JoinOn joinOn = method.getAnnotation(JoinOn.class);
        this.joinOnGroup = joinOn != null ? joinOn.group() : null;
        this.compensateMethod = findCompensateMethod(flowDefinition.getClass(), method.getName());
        this.cancelMethod = findCancelMethod(flowDefinition.getClass(), method.getName());
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
    public int getTimeoutSeconds() {
        return stepAnnotation.timeoutSeconds();
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

    public boolean isParallel() { return parallelGroup != null; }
    public String getParallelGroup() { return parallelGroup; }
    public boolean isJoinPoint() { return joinOnGroup != null; }
    public String getJoinOnGroup() { return joinOnGroup; }

    public RetryOn getRetryOn() { return getAnnotation(RetryOn.class); }
    public FailOn getFailOn() { return getAnnotation(FailOn.class); }
    public RecoverOn[] getRecoverOns() {
        RecoverOn[] methodLevel = method.getAnnotationsByType(RecoverOn.class);
        if (methodLevel.length > 0) return methodLevel;
        return flowDefinition.getClass().getAnnotationsByType(RecoverOn.class);
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
            Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
            log.error("[Step:{}] Compensation failed: {}", stepName, cause.getMessage());
            throw new RuntimeException("Compensation failed for step " + stepName, cause);
        }
    }

    public boolean hasCancellation() {
        return cancelMethod != null || compensateMethod != null;
    }

    /**
     * Run cancellation handler. Falls back to @Compensate if no @OnCancel defined.
     */
    public void cancel(F flow) {
        Method handler = cancelMethod != null ? cancelMethod : compensateMethod;
        if (handler == null) {
            log.warn("[Step:{}] No @OnCancel or @Compensate defined, skipping cancellation", stepName);
            return;
        }
        try {
            String type = cancelMethod != null ? "@OnCancel" : "@Compensate (fallback)";
            log.info("[Step:{}] Executing cancellation via {}", stepName, type);
            handler.invoke(flowDefinition, flow);
        } catch (Exception e) {
            Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
            log.error("[Step:{}] Cancellation failed: {}", stepName, cause.getMessage());
            throw new RuntimeException("Cancellation failed for step " + stepName, cause);
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

    private static Method findCancelMethod(Class<?> clazz, String stepMethodName) {
        for (Method m : clazz.getDeclaredMethods()) {
            OnCancel cancel = m.getAnnotation(OnCancel.class);
            if (cancel != null && cancel.step().equals(stepMethodName)) {
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
