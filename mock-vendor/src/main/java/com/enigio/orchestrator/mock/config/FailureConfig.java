package com.enigio.orchestrator.mock.config;

import com.enigio.orchestrator.mock.model.FailureScenario;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Component
public class FailureConfig {

    private final Map<String, FailureScenario> endpointFailures = new ConcurrentHashMap<>();

    /** Probability of failure for FLAKY mode (0.0 to 1.0). Default 0.3 = 30%. */
    private volatile double flakyRate = 0.3;

    public FailureScenario getFailureFor(String endpoint) {
        return endpointFailures.getOrDefault(endpoint, FailureScenario.NONE);
    }

    public void setFailureFor(String endpoint, FailureScenario scenario) {
        endpointFailures.put(endpoint, scenario);
    }

    public void reset() {
        endpointFailures.clear();
        flakyRate = 0.3;
    }
}
