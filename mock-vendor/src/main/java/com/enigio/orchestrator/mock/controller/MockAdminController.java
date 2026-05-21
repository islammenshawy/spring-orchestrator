package com.enigio.orchestrator.mock.controller;

import com.enigio.orchestrator.mock.config.FailureConfig;
import com.enigio.orchestrator.mock.model.FailureScenario;
import com.enigio.orchestrator.mock.service.MockDocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class MockAdminController {

    private final FailureConfig failureConfig;
    private final MockDocumentService service;

    @PostMapping("/failure-config")
    public ResponseEntity<Map<String, String>> setFailure(@RequestBody Map<String, String> config) {
        config.forEach((endpoint, scenario) ->
                failureConfig.setFailureFor(endpoint, FailureScenario.valueOf(scenario)));
        return ResponseEntity.ok(Map.of("status", "configured"));
    }

    @GetMapping("/failure-config")
    public ResponseEntity<Map<String, FailureScenario>> getFailureConfig() {
        return ResponseEntity.ok(failureConfig.getEndpointFailures());
    }

    @PostMapping("/flaky-rate")
    public ResponseEntity<Map<String, Object>> setFlakyRate(@RequestBody Map<String, Object> body) {
        double rate = ((Number) body.get("rate")).doubleValue();
        failureConfig.setFlakyRate(rate);
        return ResponseEntity.ok(Map.of("flakyRate", rate));
    }

    @GetMapping("/flaky-rate")
    public ResponseEntity<Map<String, Object>> getFlakyRate() {
        return ResponseEntity.ok(Map.of("flakyRate", failureConfig.getFlakyRate()));
    }

    @PostMapping("/reset")
    public ResponseEntity<Map<String, String>> reset() {
        service.resetAll();
        return ResponseEntity.ok(Map.of("status", "reset"));
    }
}
