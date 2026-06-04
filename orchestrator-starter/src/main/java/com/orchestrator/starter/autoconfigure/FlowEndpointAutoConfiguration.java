package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@AutoConfiguration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "orchestrator.endpoints.enabled", havingValue = "true", matchIfMissing = true)
public class FlowEndpointAutoConfiguration {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @RestController
    @ConditionalOnMissingBean(name = "flowEndpointController")
    @RequestMapping("${orchestrator.endpoints.base-path:/flows}")
    public static class FlowEndpointController {

        @Autowired private FlowTypeRegistry registry;
        @Autowired private ObjectMapper objectMapper;
        @Autowired(required = false) private jakarta.validation.Validator validator;
        @Autowired private com.orchestrator.starter.audit.StepExecutionLogRepository stepLogRepository;
        @org.springframework.beans.factory.annotation.Value("${orchestrator.search.api-enabled:false}")
        private boolean searchApiEnabled;

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

                // Validate entity if Jakarta Validator is available
                if (validator != null) {
                    var violations = validator.validate(flow);
                    if (!violations.isEmpty()) {
                        String errors = violations.stream()
                                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                                .collect(Collectors.joining(", "));
                        return ResponseEntity.badRequest().body(Map.of("error", "Validation failed: " + errors));
                    }
                }

                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                var started = orch.startFlow(flow);

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

        /**
         * Cancel a running flow. Runs @OnCancel handlers in reverse, marks CANCELLED.
         * POST /flows/{flowType}/{id}/cancel
         */
        @PostMapping("/{flowType}/{id}/cancel")
        public ResponseEntity<?> cancelFlow(
                @PathVariable String flowType,
                @PathVariable String id,
                @RequestBody(required = false) Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                String reason = body != null ? (String) body.getOrDefault("reason", "user requested") : "user requested";

                OrchestratorFlow cancelled = orch.cancelFlow(id, reason);
                if (cancelled == null) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "error", "Cannot cancel flow — not in cancellable state",
                            "flowId", id
                    ));
                }

                return ResponseEntity.ok(Map.of(
                        "flowId", cancelled.getId(),
                        "flowType", flowType,
                        "status", cancelled.getStatus().name(),
                        "currentStep", cancelled.getCurrentStep() != null ? cancelled.getCurrentStep() : "",
                        "message", "Flow cancelled. " + cancelled.getErrorMessage()
                ));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        /**
         * Search flows by @SearchAttribute fields.
         * GET /flows/{flowType}/search?field=value&field2=value2
         * Enabled via orchestrator.search.api-enabled=true (default: false)
         */
        @GetMapping("/{flowType}/search")
        public ResponseEntity<?> searchFlows(
                @PathVariable String flowType,
                @RequestParam Map<String, String> params) {
            if (!searchApiEnabled) {
                return ResponseEntity.status(403).body(Map.of(
                        "error", "Search API is disabled. Set orchestrator.search.api-enabled=true"));
            }
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                var searchParams = new java.util.LinkedHashMap<String, Object>(params);
                var results = orch.findFlows(searchParams);
                return ResponseEntity.ok(Map.of(
                        "flowType", flowType,
                        "count", results.size(),
                        "flows", results));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        /**
         * Send a signal to a running flow.
         * POST /flows/{flowType}/{id}/signal
         * Body: { "signalName": "approve", "payload": { ... } }
         */
        @PostMapping("/{flowType}/{id}/signal")
        public ResponseEntity<?> signalFlow(
                @PathVariable String flowType,
                @PathVariable String id,
                @RequestBody Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                String signalName = (String) body.get("signalName");
                if (signalName == null || signalName.isBlank()) {
                    return ResponseEntity.badRequest().body(Map.of("error", "signalName is required"));
                }
                Object payload = body.getOrDefault("payload", Map.of());
                orch.signal(id, signalName, payload);
                return ResponseEntity.ok(Map.of(
                        "flowId", id,
                        "signal", signalName,
                        "message", "Signal delivered"));
            } catch (IllegalArgumentException | IllegalStateException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            } catch (RuntimeException e) {
                return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
            }
        }

        /**
         * Retry compensation for a flow stuck in COMPENSATION_FAILED status.
         * POST /flows/{flowType}/{id}/retry-compensation
         */
        @PostMapping("/{flowType}/{id}/retry-compensation")
        public ResponseEntity<?> retryCompensation(
                @PathVariable String flowType,
                @PathVariable String id) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                OrchestratorFlowRepository repo = desc.getRepository();
                OrchestratorFlow flow = (OrchestratorFlow) repo.findById(id).orElse(null);
                if (flow == null) return ResponseEntity.notFound().build();

                if (flow.getStatus() != FlowStatus.COMPENSATION_FAILED) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "error", "Flow is not in COMPENSATION_FAILED status (current: " + flow.getStatus() + ")",
                            "flowId", id));
                }

                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();
                orch.retryCompensation(id);

                OrchestratorFlow updated = (OrchestratorFlow) repo.findById(id).orElse(flow);
                return ResponseEntity.ok(Map.of(
                        "flowId", id,
                        "status", updated.getStatus().name(),
                        "message", "Compensation retried"));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }
        // ========== Replay ==========

        /**
         * Replay a single flow.
         * POST /flows/{flowType}/{id}/replay
         * Body (optional): { "fromStep": "STEP_NAME", "allowCompleted": true }
         */
        @PostMapping("/{flowType}/{id}/replay")
        public ResponseEntity<?> replayFlow(
                @PathVariable String flowType,
                @PathVariable String id,
                @RequestBody(required = false) Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();

                var options = com.orchestrator.starter.flow.ReplayOptions.builder();
                if (body != null) {
                    if (body.containsKey("fromStep")) options.fromStep((String) body.get("fromStep"));
                    if (Boolean.TRUE.equals(body.get("allowCompleted"))) options.allowCompleted(true);
                }

                OrchestratorFlow replayed = orch.replayFlow(id, options.build());
                return ResponseEntity.ok(Map.of(
                        "flowId", replayed.getId(),
                        "status", replayed.getStatus().name(),
                        "currentStep", replayed.getCurrentStep(),
                        "message", "Flow replayed"));
            } catch (IllegalArgumentException | IllegalStateException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        /**
         * Batch replay multiple flows.
         * POST /flows/{flowType}/batch-replay
         * Body: { "flowIds": ["id1", "id2"], "fromStep": "STEP_A", "allowCompleted": false }
         */
        @PostMapping("/{flowType}/ops/batch-replay")
        public ResponseEntity<?> replayFlows(
                @PathVariable String flowType,
                @RequestBody Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();

                @SuppressWarnings("unchecked")
                java.util.List<String> flowIds = (java.util.List<String>) body.get("flowIds");
                if (flowIds == null || flowIds.isEmpty()) {
                    return ResponseEntity.badRequest().body(Map.of("error", "flowIds is required"));
                }

                var options = com.orchestrator.starter.flow.ReplayOptions.builder();
                if (body.containsKey("fromStep")) options.fromStep((String) body.get("fromStep"));
                if (Boolean.TRUE.equals(body.get("allowCompleted"))) options.allowCompleted(true);

                var results = orch.replayFlows(flowIds, options.build());
                long succeeded = results.stream().filter(r -> "replayed".equals(((java.util.Map<?,?>) r).get("status"))).count();
                return ResponseEntity.ok(Map.of(
                        "total", flowIds.size(),
                        "succeeded", succeeded,
                        "failed", flowIds.size() - succeeded,
                        "results", results));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        /**
         * Batch cancel multiple flows.
         * POST /flows/{flowType}/batch-cancel
         * Body: { "flowIds": ["id1", "id2"], "reason": "bulk cleanup" }
         */
        @PostMapping("/{flowType}/ops/batch-cancel")
        public ResponseEntity<?> cancelFlows(
                @PathVariable String flowType,
                @RequestBody Map<String, Object> body) {
            try {
                FlowTypeDescriptor desc = registry.resolve(flowType);
                FlowOrchestrator orch = (FlowOrchestrator) desc.getOrchestrator();

                @SuppressWarnings("unchecked")
                java.util.List<String> flowIds = (java.util.List<String>) body.get("flowIds");
                if (flowIds == null || flowIds.isEmpty()) {
                    return ResponseEntity.badRequest().body(Map.of("error", "flowIds is required"));
                }
                String reason = (String) body.getOrDefault("reason", "batch cancel");

                var results = orch.cancelFlows(flowIds, reason);
                long succeeded = results.stream().filter(r -> "cancelled".equals(((java.util.Map<?,?>) r).get("status"))).count();
                return ResponseEntity.ok(Map.of(
                        "total", flowIds.size(),
                        "succeeded", succeeded,
                        "failed", flowIds.size() - succeeded,
                        "results", results));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        /** Step timeline — shows step progression with timing for a specific flow. */
        @GetMapping("/{flowType}/{id}/timeline")
        public ResponseEntity<?> getTimeline(@PathVariable String flowType, @PathVariable String id) {
            var logs = stepLogRepository.findByFlowIdOrderByStartedAtAsc(id);
            if (logs.isEmpty()) return ResponseEntity.notFound().build();

            // Deduplicate: keep only the final outcome per step (last attempt)
            var stepMap = new java.util.LinkedHashMap<String, Map<String, Object>>();
            for (var log : logs) {
                String key = log.getStepName();
                var entry = new java.util.LinkedHashMap<String, Object>();
                entry.put("step", log.getStepName());
                entry.put("status", log.getStatus());
                entry.put("attempt", log.getAttemptNumber());
                entry.put("durationMs", log.getDurationMs());
                entry.put("startedAt", log.getStartedAt());
                entry.put("completedAt", log.getCompletedAt());
                if (log.getErrorMessage() != null) entry.put("error", log.getErrorMessage());

                // Keep last outcome per step, but track total attempts
                if (stepMap.containsKey(key)) {
                    var prev = stepMap.get(key);
                    int prevAttempts = (int) prev.getOrDefault("totalAttempts", 1);
                    entry.put("totalAttempts", prevAttempts + 1);
                } else {
                    entry.put("totalAttempts", 1);
                }
                stepMap.put(key, entry);
            }

            // Calculate time between steps
            var timeline = new java.util.ArrayList<>(stepMap.values());
            for (int i = 1; i < timeline.size(); i++) {
                var prev = timeline.get(i - 1);
                var curr = timeline.get(i);
                var prevCompleted = (java.time.Instant) prev.get("completedAt");
                var currStarted = (java.time.Instant) curr.get("startedAt");
                if (prevCompleted != null && currStarted != null) {
                    long gapMs = java.time.Duration.between(prevCompleted, currStarted).toMillis();
                    curr.put("gapFromPreviousMs", gapMs);
                }
            }

            // Total duration
            var first = logs.get(0);
            var last = logs.get(logs.size() - 1);
            long totalMs = 0;
            if (first.getStartedAt() != null && last.getCompletedAt() != null) {
                totalMs = java.time.Duration.between(first.getStartedAt(), last.getCompletedAt()).toMillis();
            }

            return ResponseEntity.ok(Map.of(
                    "flowId", id,
                    "totalDurationMs", totalMs,
                    "totalSteps", timeline.size(),
                    "totalLogEntries", logs.size(),
                    "steps", timeline));
        }
    }
}
