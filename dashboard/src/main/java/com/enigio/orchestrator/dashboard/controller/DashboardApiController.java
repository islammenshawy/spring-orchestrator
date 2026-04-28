package com.enigio.orchestrator.dashboard.controller;

import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
public class DashboardApiController {

    private final MongoTemplate mongoTemplate;

    // ========== Flow queries (reads from any collection via MongoTemplate) ==========

    @GetMapping("/flows")
    public ResponseEntity<List<Document>> getAllFlows(
            @RequestParam(defaultValue = "enigio_flows") String collection) {
        Query query = new Query().with(Sort.by(Sort.Direction.DESC, "createdAt")).limit(100);
        return ResponseEntity.ok(normalizeIds(mongoTemplate.find(query, Document.class, collection)));
    }

    @GetMapping("/flows/{id}")
    public ResponseEntity<?> getFlow(@PathVariable String id,
            @RequestParam(defaultValue = "enigio_flows") String collection) {
        Document doc = mongoTemplate.findById(id, Document.class, collection);
        return doc != null ? ResponseEntity.ok(normalizeId(doc)) : ResponseEntity.notFound().build();
    }

    @GetMapping("/flows/status/{status}")
    public ResponseEntity<List<Document>> getFlowsByStatus(@PathVariable String status,
            @RequestParam(defaultValue = "enigio_flows") String collection) {
        Query query = new Query(Criteria.where("status").is(status));
        return ResponseEntity.ok(normalizeIds(mongoTemplate.find(query, Document.class, collection)));
    }

    // ========== Start flows (library sample-app on port 8085) ==========

    @PostMapping("/flows/start")
    public ResponseEntity<Map<String, Object>> startFlow(@RequestBody Map<String, String> request) {
        try {
            WebClient client = WebClient.create("http://localhost:8085");
            String response = client.post()
                    .uri("/flows")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return ResponseEntity.ok(Map.of("status", "started", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    // ========== Legacy pattern endpoints (demo profile only) ==========

    @PostMapping("/flows/saga")
    public ResponseEntity<Map<String, Object>> startSagaFlow(@RequestBody Map<String, String> request) {
        return proxyPost("http://localhost:8082", "/saga/flows", request, "saga");
    }

    @PostMapping("/flows/statemachine")
    public ResponseEntity<Map<String, Object>> startSmFlow(@RequestBody Map<String, String> request) {
        return proxyPost("http://localhost:8083", "/sm/flows", request, "statemachine");
    }

    @PostMapping("/flows/spring-integration")
    public ResponseEntity<Map<String, Object>> startSiFlow(@RequestBody Map<String, String> request) {
        return proxyPost("http://localhost:8084", "/si/flows", request, "spring-integration");
    }

    // ========== Failure simulation ==========

    @PostMapping("/failure-config")
    public ResponseEntity<Map<String, Object>> configureFailure(@RequestBody Map<String, String> config) {
        try {
            WebClient client = WebClient.create("http://localhost:8081");
            String response = client.post().uri("/admin/failure-config")
                    .bodyValue(config).retrieve().bodyToMono(String.class).block();
            return ResponseEntity.ok(Map.of("status", "configured", "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    @GetMapping("/failure-config")
    public ResponseEntity<String> getFailureConfig() {
        try {
            return ResponseEntity.ok(WebClient.create("http://localhost:8081")
                    .get().uri("/admin/failure-config")
                    .retrieve().bodyToMono(String.class).block());
        } catch (Exception e) {
            return ResponseEntity.ok("{}");
        }
    }

    @PostMapping("/failure-reset")
    public ResponseEntity<Map<String, String>> resetFailures() {
        try {
            WebClient.create("http://localhost:8081")
                    .post().uri("/admin/reset").retrieve().bodyToMono(String.class).block();
            return ResponseEntity.ok(Map.of("status", "reset"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    // ========== Load test (hits all pattern apps simultaneously) ==========

    private record PatternTarget(String name, String baseUrl, String path) {}

    private static final List<PatternTarget> LOAD_TEST_TARGETS = List.of(
            new PatternTarget("sequential", "http://localhost:8085", "/flows/enigio-document"),
            new PatternTarget("parallel", "http://localhost:8085", "/flows/parallel-document")
    );

    @PostMapping("/flows/loadtest")
    public ResponseEntity<Map<String, Object>> loadTest(@RequestBody Map<String, Object> request) {
        int count = (int) request.getOrDefault("count", 10);
        Map<String, Integer> results = new java.util.LinkedHashMap<>();

        for (PatternTarget target : LOAD_TEST_TARGETS) {
            int started = 0;
            WebClient client = WebClient.create(target.baseUrl());
            for (int i = 0; i < count; i++) {
                Map<String, String> flowReq = new java.util.HashMap<>(Map.of(
                        "title", target.name() + " #" + (i + 1)
                ));
                // Sequential flows need extra fields
                if ("sequential".equals(target.name())) {
                    flowReq.put("content", "Auto-generated");
                    flowReq.put("signerEmail", "load" + i + "@test.com");
                }
                try {
                    client.post().uri(target.path()).bodyValue(flowReq)
                            .retrieve().bodyToMono(String.class).subscribe();
                    started++;
                } catch (Exception e) {
                    // Target app may not be running — skip
                }
            }
            results.put(target.name(), started);
        }

        int total = results.values().stream().mapToInt(Integer::intValue).sum();
        return ResponseEntity.ok(Map.of(
                "status", "launched",
                "flowsStarted", total,
                "perPattern", results
        ));
    }

    // ========== Helpers ==========

    private ResponseEntity<Map<String, Object>> proxyPost(String baseUrl, String path,
            Map<String, String> request, String pattern) {
        try {
            String response = WebClient.create(baseUrl).post().uri(path)
                    .bodyValue(request).retrieve().bodyToMono(String.class).block();
            return ResponseEntity.ok(Map.of("status", "started", "pattern", pattern, "response", response));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /** Convert ObjectId _id to hex string so JSON serialization works correctly. */
    private List<Document> normalizeIds(List<Document> docs) {
        docs.forEach(this::normalizeId);
        return docs;
    }

    private Document normalizeId(Document doc) {
        Object id = doc.get("_id");
        if (id instanceof ObjectId oid) {
            doc.put("_id", oid.toHexString());
        }
        return doc;
    }
}
