package com.enigio.orchestrator.dashboard.controller;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.domain.FlowStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
public class DashboardApiController {

    private final DocumentFlowRepository flowRepository;

    @GetMapping("/flows")
    public ResponseEntity<List<DocumentFlow>> getAllFlows() {
        List<DocumentFlow> flows = flowRepository.findAll(Sort.by(Sort.Direction.DESC, "createdAt"));
        return ResponseEntity.ok(flows);
    }

    @GetMapping("/flows/{id}")
    public ResponseEntity<DocumentFlow> getFlow(@PathVariable String id) {
        return flowRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/flows/status/{status}")
    public ResponseEntity<List<DocumentFlow>> getFlowsByStatus(@PathVariable String status) {
        FlowStatus flowStatus = FlowStatus.valueOf(status.toUpperCase());
        return ResponseEntity.ok(flowRepository.findByStatus(flowStatus));
    }

    @PostMapping("/flows/saga")
    public ResponseEntity<Map<String, Object>> startSagaFlow(@RequestBody Map<String, String> request) {
        try {
            WebClient client = WebClient.create("http://localhost:8082");
            String response = client.post()
                    .uri("/saga/flows")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(Map.of("status", "started", "pattern", "saga", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @PostMapping("/flows/statemachine")
    public ResponseEntity<Map<String, Object>> startSmFlow(@RequestBody Map<String, String> request) {
        try {
            WebClient client = WebClient.create("http://localhost:8083");
            String response = client.post()
                    .uri("/sm/flows")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(Map.of("status", "started", "pattern", "statemachine", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @PostMapping("/flows/spring-integration")
    public ResponseEntity<Map<String, Object>> startSiFlow(@RequestBody Map<String, String> request) {
        try {
            WebClient client = WebClient.create("http://localhost:8084");
            String response = client.post()
                    .uri("/si/flows")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(Map.of("status", "started", "pattern", "spring-integration", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @PostMapping("/failure-config")
    public ResponseEntity<Map<String, Object>> configureFailure(@RequestBody Map<String, String> config) {
        try {
            WebClient client = WebClient.create("http://localhost:8081");
            String response = client.post()
                    .uri("/admin/failure-config")
                    .bodyValue(config)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(Map.of("status", "configured", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @GetMapping("/failure-config")
    public ResponseEntity<String> getFailureConfig() {
        try {
            WebClient client = WebClient.create("http://localhost:8081");
            String response = client.get()
                    .uri("/admin/failure-config")
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.ok("{}");
        }
    }

    @PostMapping("/failure-reset")
    public ResponseEntity<Map<String, String>> resetFailures() {
        try {
            WebClient client = WebClient.create("http://localhost:8081");
            client.post().uri("/admin/reset").retrieve().bodyToMono(String.class).block();
            return ResponseEntity.ok(Map.of("status", "reset"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @PostMapping("/flows/{id}/retry")
    public ResponseEntity<Map<String, String>> retryFlow(@PathVariable String id) {
        return flowRepository.findById(id).map(flow -> {
            flow.setStatus(FlowStatus.IN_PROGRESS);
            flow.setRetryCount(0);
            flow.setBackoffSeconds(0);
            flow.setNextRetryAt(null);
            flow.setErrorMessage(null);
            flowRepository.save(flow);
            return ResponseEntity.ok(Map.of("status", "retrying", "flowId", id));
        }).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/flows/loadtest")
    public ResponseEntity<Map<String, Object>> loadTest(@RequestBody Map<String, Object> request) {
        int count = (int) request.getOrDefault("count", 5);
        String pattern = (String) request.getOrDefault("pattern", "both");
        int started = 0;

        WebClient sagaClient = WebClient.create("http://localhost:8082");
        WebClient smClient = WebClient.create("http://localhost:8083");
        WebClient siClient = WebClient.create("http://localhost:8084");

        for (int i = 0; i < count; i++) {
            Map<String, String> flowReq = Map.of(
                    "title", "Load Test #" + (i + 1),
                    "content", "Auto-generated load test content",
                    "signerEmail", "loadtest" + i + "@example.com",
                    "metadata", "{\"loadTest\": true, \"index\": " + i + "}"
            );
            try {
                if ("saga".equals(pattern) || "all".equals(pattern) || "both".equals(pattern)) {
                    sagaClient.post().uri("/saga/flows").bodyValue(flowReq)
                            .retrieve().bodyToMono(String.class).subscribe();
                    started++;
                }
                if ("statemachine".equals(pattern) || "all".equals(pattern) || "both".equals(pattern)) {
                    smClient.post().uri("/sm/flows").bodyValue(flowReq)
                            .retrieve().bodyToMono(String.class).subscribe();
                    started++;
                }
                if ("spring-integration".equals(pattern) || "all".equals(pattern)) {
                    siClient.post().uri("/si/flows").bodyValue(flowReq)
                            .retrieve().bodyToMono(String.class).subscribe();
                    started++;
                }
            } catch (Exception e) {
                // Continue with remaining flows
            }
        }

        return ResponseEntity.ok(Map.of(
                "status", "launched",
                "flowsStarted", started,
                "pattern", pattern
        ));
    }
}
