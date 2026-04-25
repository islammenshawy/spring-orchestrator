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

import java.util.Map;

/**
 * Auto-exposes REST endpoints for flows.
 *
 * POST {base-path}              — start flow async (returns ID immediately, execution via Kafka)
 * GET  {base-path}/{id}         — get flow by ID (poll for status)
 * GET  {base-path}/{id}/status  — get status only (lightweight)
 *
 * The POST is async by design:
 *   1. Saves flow to MongoDB
 *   2. Writes outbox event (same DB)
 *   3. Returns 202 Accepted + flow ID + correlationId
 *   4. Outbox publisher sends to Kafka (background, ~500ms)
 *   5. Kafka consumer executes steps (async, separate thread/pod)
 *
 * Caller gets the ID immediately and polls GET /flows/{id} for completion.
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

        /**
         * Start a flow asynchronously.
         *
         * Returns 202 Accepted with the flow ID and correlationId immediately.
         * The actual step execution happens via Kafka in the background.
         *
         * Caller polls GET /flows/{id} or GET /flows/{id}/status for completion.
         */
        @PostMapping
        public ResponseEntity<?> startFlow(@RequestBody Object flowEntity) {
            if (flowEntity instanceof OrchestratorFlow flow) {
                var saved = repository.save(flow);
                var started = orchestrator.startFlow((OrchestratorFlow) saved);

                return ResponseEntity.accepted().body(Map.of(
                        "id", started.getId(),
                        "correlationId", started.getCorrelationId(),
                        "status", started.getStatus().name(),
                        "currentStep", started.getCurrentStep(),
                        "message", "Flow started. Poll GET /flows/" + started.getId() + " for status."
                ));
            }
            return ResponseEntity.badRequest().body(
                    Map.of("error", "Request body must be a flow entity"));
        }

        /**
         * Get full flow state including domain fields.
         */
        @GetMapping("/{id}")
        public ResponseEntity<?> getFlow(@PathVariable String id) {
            Object found = repository.findById(id).orElse(null);
            return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
        }

        /**
         * Lightweight status check — only returns orchestrator fields.
         */
        @GetMapping("/{id}/status")
        public ResponseEntity<?> getStatus(@PathVariable String id) {
            Object found = repository.findById(id).orElse(null);
            if (found == null) return ResponseEntity.notFound().build();

            OrchestratorFlow flow = (OrchestratorFlow) found;
            return ResponseEntity.ok(Map.of(
                    "id", flow.getId(),
                    "status", flow.getStatus().name(),
                    "currentStep", flow.getCurrentStep() != null ? flow.getCurrentStep() : "",
                    "retryCount", flow.getRetryCount(),
                    "errorMessage", flow.getErrorMessage() != null ? flow.getErrorMessage() : ""
            ));
        }

        /**
         * Get flow by correlation ID.
         */
        @GetMapping("/correlation/{correlationId}")
        public ResponseEntity<?> getByCorrelation(@PathVariable String correlationId) {
            Object found = repository.findByCorrelationId(correlationId).orElse(null);
            return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
        }
    }
}
