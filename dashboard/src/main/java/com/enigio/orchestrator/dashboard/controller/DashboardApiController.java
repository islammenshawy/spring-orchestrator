package com.enigio.orchestrator.dashboard.controller;

import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
public class DashboardApiController {

    private final MongoTemplate mongoTemplate;
    private final MongoTemplate disMongoTemplate;

    @Value("${dis.base-url:http://localhost:8087}")
    private String disBaseUrl;

    public DashboardApiController(MongoTemplate mongoTemplate,
                                  @Qualifier("disMongoTemplate") MongoTemplate disMongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.disMongoTemplate = disMongoTemplate;
    }

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

    // ========== DIS (Digital Instrument Service) ==========

    @GetMapping("/dis/flows")
    public ResponseEntity<List<Document>> getDisFlows() {
        Query query = new Query().with(Sort.by(Sort.Direction.DESC, "createdAt")).limit(100);
        return ResponseEntity.ok(normalizeIds(disMongoTemplate.find(query, Document.class, "dis_instrument_flows")));
    }

    @GetMapping("/dis/flows/{id}")
    public ResponseEntity<?> getDisFlow(@PathVariable String id) {
        Document doc = disMongoTemplate.findById(id, Document.class, "dis_instrument_flows");
        return doc != null ? ResponseEntity.ok(normalizeId(doc)) : ResponseEntity.notFound().build();
    }

    @PostMapping("/dis/flows/start")
    public ResponseEntity<Map<String, Object>> startDisFlow(@RequestBody Map<String, Object> request) {
        try {
            WebClient client = WebClient.create(disBaseUrl);
            String response = client.post()
                    .uri("/flows/enigio-instrument")
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

    @GetMapping("/dis/flows/{flowId}/steps")
    public ResponseEntity<List<Document>> getDisStepLogs(@PathVariable String flowId) {
        Query query = new Query(Criteria.where("flowId").is(flowId))
                .with(Sort.by(Sort.Direction.ASC, "startedAt"));
        return ResponseEntity.ok(disMongoTemplate.find(query, Document.class, "orchestrator_step_log"));
    }

    @GetMapping("/dis/mongo/collections")
    public ResponseEntity<List<String>> getDisCollections() {
        return ResponseEntity.ok(disMongoTemplate.getCollectionNames().stream().sorted().toList());
    }

    @GetMapping("/dis/mongo/collections/{name}")
    public ResponseEntity<Map<String, Object>> getDisCollectionData(
            @PathVariable String name,
            @RequestParam(defaultValue = "0") int skip,
            @RequestParam(defaultValue = "50") int limit) {
        var docs = disMongoTemplate.getCollection(name).find()
                .sort(new Document("_id", -1)).skip(skip).limit(limit)
                .into(new ArrayList<>());
        long count = disMongoTemplate.getCollection(name).countDocuments();
        return ResponseEntity.ok(Map.of("collection", name, "total", count,
                "skip", skip, "limit", limit, "documents", docs));
    }

    // ========== Load test ==========

    private record FlowTarget(String name, String baseUrl, String path) {}

    private final Map<String, FlowTarget> flowTargets() {
        return Map.of(
                "sequential", new FlowTarget("sequential", "http://localhost:8085", "/flows/enigio-document"),
                "parallel", new FlowTarget("parallel", "http://localhost:8085", "/flows/parallel-document"),
                "instrument", new FlowTarget("instrument", disBaseUrl, "/flows/enigio-instrument")
        );
    }

    // Sustained load state
    private volatile boolean sustainedRunning = false;
    private volatile int sustainedLaunched = 0;
    private volatile int sustainedErrors = 0;

    /** Burst load test — launch N flows immediately */
    @PostMapping("/flows/loadtest")
    public ResponseEntity<Map<String, Object>> loadTest(@RequestBody Map<String, Object> request) {
        int count = (int) request.getOrDefault("count", 10);
        @SuppressWarnings("unchecked")
        List<String> flowTypes = (List<String>) request.getOrDefault("flowTypes",
                List.of("sequential", "parallel"));

        Map<String, Integer> results = new java.util.LinkedHashMap<>();

        for (String type : flowTypes) {
            FlowTarget target = flowTargets().get(type);
            if (target == null) continue;
            WebClient client = WebClient.create(target.baseUrl());
            int started = 0;
            for (int i = 0; i < count; i++) {
                try {
                    client.post().uri(target.path())
                            .bodyValue(buildFlowRequest(type, i))
                            .retrieve().bodyToMono(String.class).subscribe();
                    started++;
                } catch (Exception e) { /* skip */ }
            }
            results.put(type, started);
        }

        int total = results.values().stream().mapToInt(Integer::intValue).sum();
        return ResponseEntity.ok(Map.of(
                "status", "launched",
                "flowsStarted", total,
                "perPattern", results
        ));
    }

    /** Sustained load — constant rate for a duration */
    @PostMapping("/flows/loadtest/sustained")
    public ResponseEntity<Map<String, Object>> startSustainedLoad(@RequestBody Map<String, Object> request) {
        if (sustainedRunning) {
            return ResponseEntity.ok(Map.of("status", "already_running",
                    "launched", sustainedLaunched, "errors", sustainedErrors));
        }

        int durationSec = (int) request.getOrDefault("durationSeconds", 60);
        int ratePerSec = (int) request.getOrDefault("ratePerSecond", 2);
        @SuppressWarnings("unchecked")
        List<String> flowTypes = (List<String>) request.getOrDefault("flowTypes",
                List.of("sequential", "parallel"));

        sustainedRunning = true;
        sustainedLaunched = 0;
        sustainedErrors = 0;

        Thread.startVirtualThread(() -> {
            long endTime = System.currentTimeMillis() + (durationSec * 1000L);
            int counter = 0;

            while (System.currentTimeMillis() < endTime && sustainedRunning) {
                for (int r = 0; r < ratePerSec && sustainedRunning; r++) {
                    String type = flowTypes.get(counter % flowTypes.size());
                    FlowTarget target = flowTargets().get(type);
                    if (target == null) continue;
                    try {
                        WebClient.create(target.baseUrl()).post().uri(target.path())
                                .bodyValue(buildFlowRequest(type, counter))
                                .retrieve().bodyToMono(String.class).subscribe();
                        sustainedLaunched++;
                    } catch (Exception e) {
                        sustainedErrors++;
                    }
                    counter++;
                }
                try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
            }
            sustainedRunning = false;
        });

        return ResponseEntity.ok(Map.of(
                "status", "started",
                "durationSeconds", durationSec,
                "ratePerSecond", ratePerSec,
                "flowTypes", flowTypes
        ));
    }

    @PostMapping("/flows/loadtest/stop")
    public ResponseEntity<Map<String, Object>> stopSustainedLoad() {
        sustainedRunning = false;
        return ResponseEntity.ok(Map.of(
                "status", "stopped",
                "totalLaunched", sustainedLaunched,
                "errors", sustainedErrors
        ));
    }

    @GetMapping("/flows/loadtest/status")
    public ResponseEntity<Map<String, Object>> getSustainedStatus() {
        return ResponseEntity.ok(Map.of(
                "running", sustainedRunning,
                "launched", sustainedLaunched,
                "errors", sustainedErrors
        ));
    }

    private Map<String, Object> buildFlowRequest(String type, int index) {
        Map<String, Object> req = new java.util.HashMap<>();
        if ("instrument".equals(type)) {
            String[] instruments = {"PROMISSORY_NOTE", "BILL_OF_LADING", "GUARANTEE", "CERTIFICATE"};
            req.put("reference", "LOAD-" + type.toUpperCase() + "-" + (index + 1));
            req.put("title", "Load Test Instrument #" + (index + 1));
            req.put("content", "Auto-generated load test content");
            req.put("instrumentType", instruments[index % instruments.length]);
            req.put("documentCode", "NEG");
            req.put("signers", List.of(Map.of(
                    "name", "Load Signer " + (index + 1),
                    "email", "signer" + index + "@loadtest.com",
                    "phone", "+4670000" + String.format("%04d", index),
                    "capacity", "CEO",
                    "organisation", "LoadTest Corp",
                    "order", 1
            )));
            req.put("recipient", Map.of(
                    "name", "Load Recipient",
                    "email", "recipient@loadtest.com"
            ));
        } else {
            req.put("title", type + " #" + (index + 1));
            if ("sequential".equals(type)) {
                req.put("content", "Auto-generated");
                req.put("signerEmail", "load" + index + "@test.com");
            }
        }
        return req;
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
