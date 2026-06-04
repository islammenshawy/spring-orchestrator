package com.docuseal.signing.config;

import com.docuseal.signing.model.SigningFlowEntity;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/flows/docuseal-signing")
public class SigningController {

    private final FlowTypeRegistry registry;
    private final MongoTemplate mongoTemplate;
    private final com.orchestrator.starter.audit.StepExecutionLogRepository stepLogRepository;

    @Value("${signing.party-a-email}")
    private String defaultPartyAEmail;

    @Value("${signing.party-b-email}")
    private String defaultPartyBEmail;

    public SigningController(FlowTypeRegistry registry, MongoTemplate mongoTemplate,
                             com.orchestrator.starter.audit.StepExecutionLogRepository stepLogRepository) {
        this.registry = registry;
        this.mongoTemplate = mongoTemplate;
        this.stepLogRepository = stepLogRepository;
    }

    @PostMapping
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ResponseEntity<?> startSigningFlow(@RequestBody(required = false) Map<String, Object> body) {
        SigningFlowEntity flow = new SigningFlowEntity();
        flow.setCorrelationId(UUID.randomUUID().toString());

        if (body != null) {
            flow.setReference((String) body.getOrDefault("reference", "Agreement-" + System.currentTimeMillis()));
            flow.setPartyAEmail((String) body.getOrDefault("partyAEmail", defaultPartyAEmail));
            flow.setPartyAName((String) body.getOrDefault("partyAName", "Party A"));
            flow.setPartyBEmail((String) body.getOrDefault("partyBEmail", defaultPartyBEmail));
            flow.setPartyBName((String) body.getOrDefault("partyBName", "Party B"));
        } else {
            flow.setReference("Agreement-" + System.currentTimeMillis());
            flow.setPartyAEmail(defaultPartyAEmail);
            flow.setPartyAName("Party A");
            flow.setPartyBEmail(defaultPartyBEmail);
            flow.setPartyBName("Party B");
        }

        FlowOrchestrator orch = (FlowOrchestrator) registry.resolve("docuseal-signing").getOrchestrator();
        SigningFlowEntity started = (SigningFlowEntity) orch.startFlow(flow);

        log.info("[Signing] Started flow {} for '{}'", started.getId(), started.getReference());

        return ResponseEntity.ok(Map.of(
                "id", started.getId(),
                "reference", started.getReference(),
                "status", started.getStatus().name(),
                "partyA", started.getPartyAEmail(),
                "partyB", started.getPartyBEmail()));
    }

    @GetMapping("/{id}/timeline")
    public ResponseEntity<?> getTimeline(@PathVariable String id) {
        var logs = stepLogRepository.findByFlowIdOrderByStartedAtAsc(id);
        if (logs.isEmpty()) return ResponseEntity.notFound().build();

        var stepMap = new java.util.LinkedHashMap<String, Map<String, Object>>();
        for (var log : logs) {
            var entry = new java.util.LinkedHashMap<String, Object>();
            entry.put("step", log.getStepName());
            entry.put("status", log.getStatus());
            entry.put("attempt", log.getAttemptNumber());
            entry.put("durationMs", log.getDurationMs());
            entry.put("startedAt", log.getStartedAt());
            entry.put("completedAt", log.getCompletedAt());
            if (log.getErrorMessage() != null) entry.put("error", log.getErrorMessage());
            String key = log.getStepName();
            if (stepMap.containsKey(key)) {
                int prev = (int) stepMap.get(key).getOrDefault("totalAttempts", 1);
                entry.put("totalAttempts", prev + 1);
            } else {
                entry.put("totalAttempts", 1);
            }
            stepMap.put(key, entry);
        }

        var timeline = new java.util.ArrayList<>(stepMap.values());
        for (int i = 1; i < timeline.size(); i++) {
            var prev = timeline.get(i - 1);
            var curr = timeline.get(i);
            var pc = (java.time.Instant) prev.get("completedAt");
            var cs = (java.time.Instant) curr.get("startedAt");
            if (pc != null && cs != null) {
                curr.put("gapFromPreviousMs", java.time.Duration.between(pc, cs).toMillis());
            }
        }

        long totalMs = 0;
        if (!logs.isEmpty()) {
            var first = logs.get(0);
            var last = logs.get(logs.size() - 1);
            if (first.getStartedAt() != null && last.getCompletedAt() != null)
                totalMs = java.time.Duration.between(first.getStartedAt(), last.getCompletedAt()).toMillis();
        }

        return ResponseEntity.ok(Map.of(
                "flowId", id, "totalDurationMs", totalMs,
                "totalSteps", timeline.size(), "totalLogEntries", logs.size(),
                "steps", timeline));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getFlow(@PathVariable String id) {
        SigningFlowEntity flow = mongoTemplate.findById(id, SigningFlowEntity.class);
        if (flow == null) return ResponseEntity.notFound().build();

        return ResponseEntity.ok(Map.of(
                "id", flow.getId(),
                "reference", flow.getReference(),
                "status", flow.getStatus().name(),
                "currentStep", flow.getCurrentStep() != null ? flow.getCurrentStep() : "",
                "partyAStatus", flow.getPartyAStatus() != null ? flow.getPartyAStatus() : "pending",
                "partyBStatus", flow.getPartyBStatus() != null ? flow.getPartyBStatus() : "pending",
                "signedDocumentUrl", flow.getSignedDocumentUrl() != null ? flow.getSignedDocumentUrl() : "",
                "completionNotified", flow.isCompletionNotified()));
    }
}
