package com.dis.instrument.core.api;

import com.dis.instrument.vendor.enigio.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeRegistry;
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
 * REST endpoint for downstream systems to approve, cancel, or query flow status.
 */
@Slf4j
@RestController
@RequestMapping("/flows/enigio-instrument")
@RequiredArgsConstructor
public class FlowApprovalController {

    private final MongoTemplate mongoTemplate;
    private final FlowTypeRegistry flowTypeRegistry;

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

    /**
     * Cancel a running flow.
     * Runs @OnCancel handlers in reverse (invalidate document, cancel transfer),
     * then marks as CANCELLED.
     *
     * Cancellation is NOT allowed when:
     * - Flow is already COMPLETED (document transferred)
     * - Flow is already CANCELLED or FAILED
     * - Document is inTransit on Enigio (transfer in progress, recipient may have opened)
     */
    @PostMapping("/{id}/cancel")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ResponseEntity<Map<String, Object>> cancelFlow(
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> body) {

        String reason = body != null ? (String) body.getOrDefault("reason", "user requested") : "user requested";

        try {
            FlowOrchestrator orch = (FlowOrchestrator) flowTypeRegistry.resolve("enigio-instrument").getOrchestrator();
            OrchestratorFlow cancelled = orch.cancelFlow(id, reason);

            if (cancelled == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Cannot cancel — flow not in cancellable state (must be IN_PROGRESS, WAITING_RETRY, or PENDING)",
                        "flowId", id
                ));
            }

            return ResponseEntity.ok(Map.of(
                    "flowId", cancelled.getId(),
                    "status", cancelled.getStatus().name(),
                    "message", "Flow cancelled. " + cancelled.getErrorMessage()
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage(), "flowId", id));
        }
    }
}
