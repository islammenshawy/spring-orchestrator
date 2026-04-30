package com.dis.instrument.core.api;

import com.dis.instrument.vendor.enigio.EnigioInstrumentEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST endpoint for downstream systems to approve flow progression.
 * Called after receiving a notification from the dis.instrument.notifications topic.
 *
 * Flow: Group 1 completes → notification published → downstream consumes →
 *       downstream calls POST /flows/enigio-instrument/{id}/approve → Group 2 starts
 */
@Slf4j
@RestController
@RequestMapping("/flows/enigio-instrument")
@RequiredArgsConstructor
public class FlowApprovalController {

    private final MongoTemplate mongoTemplate;

    /**
     * Approve the next phase of an instrument flow.
     * The gate step polls this flag and advances when true.
     */
    @PostMapping("/{id}/approve")
    public ResponseEntity<Map<String, Object>> approveFlow(
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> body) {

        EnigioInstrumentEntity flow = mongoTemplate.findById(id, EnigioInstrumentEntity.class);
        if (flow == null) {
            return ResponseEntity.notFound().build();
        }

        String currentStep = flow.getCurrentStep();
        String approved;

        if ("AWAIT_PREPARATION_APPROVAL".equals(currentStep)) {
            mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(id)),
                    new Update().set("signingApproved", true),
                    EnigioInstrumentEntity.class);
            approved = "signingApproved";
            log.info("[{}] Signing phase approved by downstream", id);

        } else if ("AWAIT_DELIVERY_APPROVAL".equals(currentStep)) {
            mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(id)),
                    new Update().set("deliveryApproved", true),
                    EnigioInstrumentEntity.class);
            approved = "deliveryApproved";
            log.info("[{}] Delivery phase approved by downstream", id);

        } else {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Flow is not awaiting approval",
                    "currentStep", currentStep != null ? currentStep : "unknown",
                    "flowId", id
            ));
        }

        return ResponseEntity.ok(Map.of(
                "flowId", id,
                "approved", approved,
                "currentStep", currentStep,
                "message", "Next phase will start on next retry cycle"
        ));
    }

    /**
     * Get current approval status of a flow.
     */
    @GetMapping("/{id}/approval-status")
    public ResponseEntity<Map<String, Object>> getApprovalStatus(@PathVariable String id) {
        EnigioInstrumentEntity flow = mongoTemplate.findById(id, EnigioInstrumentEntity.class);
        if (flow == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(Map.of(
                "flowId", id,
                "currentStep", flow.getCurrentStep() != null ? flow.getCurrentStep() : "",
                "status", flow.getStatus().name(),
                "preparationNotified", flow.isPreparationNotified(),
                "signingApproved", flow.isSigningApproved(),
                "signingNotified", flow.isSigningNotified(),
                "deliveryApproved", flow.isDeliveryApproved()
        ));
    }
}
