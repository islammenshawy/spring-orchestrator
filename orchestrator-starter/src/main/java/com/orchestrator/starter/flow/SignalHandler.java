package com.orchestrator.starter.flow;

import com.orchestrator.starter.domain.OrchestratorFlow;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

/**
 * Wraps a @Signal-annotated method for invocation by the library.
 */
@Slf4j
public class SignalHandler<F extends OrchestratorFlow> {

    private final Object flowDefinition;
    private final Method method;
    private final String signalName;
    private final Class<?> payloadType; // null if no payload parameter

    public SignalHandler(Object flowDefinition, Method method, String signalName) {
        this.flowDefinition = flowDefinition;
        this.method = method;
        this.signalName = signalName;
        this.method.setAccessible(true);

        // Second parameter (if any) is the payload type
        if (method.getParameterCount() == 2) {
            this.payloadType = method.getParameterTypes()[1];
        } else {
            this.payloadType = null;
        }
    }

    public String getSignalName() {
        return signalName;
    }

    public Class<?> getPayloadType() {
        return payloadType;
    }

    /**
     * Invoke the signal handler with the flow and optional payload.
     */
    @SuppressWarnings("unchecked")
    public void invoke(F flow, Object payload) {
        try {
            if (payloadType != null && payload != null) {
                method.invoke(flowDefinition, flow, payload);
            } else {
                method.invoke(flowDefinition, flow);
            }
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("[Signal:{}] Handler failed: {}", signalName, cause.getMessage());
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("Signal handler failed: " + signalName, cause);
        }
    }
}
