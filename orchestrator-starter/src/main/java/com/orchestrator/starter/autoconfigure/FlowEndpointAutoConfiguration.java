package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.UUID;

@Slf4j
@AutoConfiguration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "orchestrator.endpoints.enabled", havingValue = "true", matchIfMissing = true)
public class FlowEndpointAutoConfiguration {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @RestController
    @RequestMapping("${orchestrator.endpoints.base-path:/flows}")
    public static class FlowEndpointController {

        @Autowired private FlowTypeRegistry registry;
        @Autowired private ObjectMapper objectMapper;

        @PostConstruct
        void init() {
            log.info("Auto-endpoints: {} flow type(s): {}",
                    registry.size(), registry.getFlowTypeNames());
        }

        /**
         * Start a flow with explicit flow type.
         * POST /flows/{flowType}
         */
        @PostMapping("/{flowType}")
        public ResponseEntity<?> startFlowByType(
                @PathVariable String flowType,
                @RequestBody Map<String, Object> body) {
            return doStartFlow(flowType, body);
        }

        /**
         * Start a flow — single-flow backward compat.
         * POST /flows (only works when exactly one flow type is registered)
         */
        @PostMapping
        public ResponseEntity<?> startFlow(@RequestBody Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.getSingleOrThrow();
                return doStartFlow(desc.getFlowType(), body);
            } catch (IllegalStateException e) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Multiple flow types registered. Use POST /flows/{flowType}. Available: " +
                                registry.getFlowTypeNames()));
            }
        }

        private ResponseEntity<?> doStartFlow(String flowType, Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                body.putIfAbsent("correlationId", UUID.randomUUID().toString());
                String json = objectMapper.writeValueAsString(body);
                OrchestratorFlow flow = (OrchestratorFlow) objectMapper.readValue(json, desc.getEntityClass());
                OrchestratorFlowRepository repo = desc.getRepository();
                var saved = (OrchestratorFlow) repo.save(flow);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                var started = orch.startFlow(saved);

                return ResponseEntity.accepted().body(Map.of(
                        "id", started.getId(),
                        "correlationId", started.getCorrelationId(),
                        "flowType", flowType,
                        "status", started.getStatus().name(),
                        "currentStep", started.getCurrentStep(),
                        "message", "Flow started. Poll GET /flows/" + flowType + "/" + started.getId() + " for status."
                ));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            } catch (Exception e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        @GetMapping("/{flowType}/{id}")
        public ResponseEntity<?> getFlowByType(@PathVariable String flowType, @PathVariable String id) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                Object found = desc.getRepository().findById(id).orElse(null);
                return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
            } catch (IllegalArgumentException e) {
                // flowType might actually be an ID (single-flow backward compat)
                return getFlowById(flowType);
            }
        }

        /** Single-flow backward compat: GET /flows/{id} */
        private ResponseEntity<?> getFlowById(String id) {
            try {
                FlowTypeDescriptor desc = registry.getSingleOrThrow();
                Object found = desc.getRepository().findById(id).orElse(null);
                return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
            } catch (IllegalStateException e) {
                return ResponseEntity.notFound().build();
            }
        }

        @GetMapping("/{flowType}/{id}/status")
        public ResponseEntity<?> getStatus(@PathVariable String flowType, @PathVariable String id) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                Object found = desc.getRepository().findById(id).orElse(null);
                if (found == null) return ResponseEntity.notFound().build();
                OrchestratorFlow flow = (OrchestratorFlow) found;
                return ResponseEntity.ok(Map.of(
                        "id", flow.getId(),
                        "flowType", flowType,
                        "status", flow.getStatus().name(),
                        "currentStep", flow.getCurrentStep() != null ? flow.getCurrentStep() : "",
                        "retryCount", flow.getRetryCount(),
                        "errorMessage", flow.getErrorMessage() != null ? flow.getErrorMessage() : ""
                ));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.notFound().build();
            }
        }
    }
}
