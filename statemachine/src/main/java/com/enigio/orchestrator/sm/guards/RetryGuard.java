package com.enigio.orchestrator.sm.guards;

import com.enigio.orchestrator.sm.machine.DocumentFlowEvents;
import com.enigio.orchestrator.sm.machine.DocumentFlowStates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.guard.Guard;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RetryGuard implements Guard<DocumentFlowStates, DocumentFlowEvents> {

    @Value("${enigio.client.max-retries:3}")
    private int maxRetries;

    @Override
    public boolean evaluate(StateContext<DocumentFlowStates, DocumentFlowEvents> context) {
        Integer retryCount = (Integer) context.getExtendedState().getVariables()
                .getOrDefault("retryCount", 0);

        boolean allowed = retryCount < maxRetries;
        if (allowed) {
            context.getExtendedState().getVariables().put("retryCount", retryCount + 1);
            log.info("[SM] Retry allowed: attempt {} of {}", retryCount + 1, maxRetries);
        } else {
            log.warn("[SM] Retry limit reached ({}/{})", retryCount, maxRetries);
        }
        return allowed;
    }
}
