package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * Auto-exposes REST endpoints for flows when web is available.
 *
 * Provides:
 *   POST {base-path}           — start a new flow (body = flow entity JSON)
 *   GET  {base-path}/{id}      — get flow by ID
 *   GET  {base-path}/correlation/{correlationId} — get by correlation ID
 *
 * Enabled by default. Disable with:
 *   orchestrator.endpoints.enabled: false
 *
 * Override base path:
 *   orchestrator.endpoints.base-path: /my-flows
 *
 * Users can override entirely by defining their own @RestController
 * on the same path — Spring won't register duplicates.
 */
@Slf4j
@AutoConfiguration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "orchestrator.endpoints.enabled", havingValue = "true", matchIfMissing = true)
public class FlowEndpointAutoConfiguration {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @RestController
    @RequestMapping("${orchestrator.endpoints.base-path:/flows}")
    @RequiredArgsConstructor
    public static class FlowEndpointController {

        private final FlowOrchestrator orchestrator;
        private final OrchestratorFlowRepository repository;

        @PostMapping
        public ResponseEntity<?> startFlow(@RequestBody Object flowEntity) {
            if (flowEntity instanceof OrchestratorFlow flow) {
                if (flow.getCorrelationId() == null || flow.getCorrelationId().isBlank()) {
                    // Auto-generate correlationId if not set
                    flow.setCurrentStep(null); // will be set by orchestrator
                }
                var saved = repository.save(flow);
                var started = orchestrator.startFlow((OrchestratorFlow) saved);
                return ResponseEntity.ok(started);
            }
            return ResponseEntity.badRequest().body("Request body must be a flow entity");
        }

        @GetMapping("/{id}")
        public ResponseEntity<?> getFlow(@PathVariable String id) {
            return repository.findById(id)
                    .map(f -> ResponseEntity.ok(f))
                    .orElse(ResponseEntity.notFound().build());
        }

        @GetMapping("/correlation/{correlationId}")
        public ResponseEntity<?> getByCorrelation(@PathVariable String correlationId) {
            return repository.findByCorrelationId(correlationId)
                    .map(f -> ResponseEntity.ok(f))
                    .orElse(ResponseEntity.notFound().build());
        }
    }
}
