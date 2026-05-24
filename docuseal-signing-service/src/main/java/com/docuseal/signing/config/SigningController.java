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

    @Value("${signing.party-a-email}")
    private String defaultPartyAEmail;

    @Value("${signing.party-b-email}")
    private String defaultPartyBEmail;

    public SigningController(FlowTypeRegistry registry, MongoTemplate mongoTemplate) {
        this.registry = registry;
        this.mongoTemplate = mongoTemplate;
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
