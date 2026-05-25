package com.dis.instrument.inbound;

import com.dis.instrument.model.FlowStep;
import com.dis.instrument.inbound.response.*;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.kafka.StepCommandMessage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.Map;
import java.util.UUID;

/**
 * REST endpoints for downstream systems to approve, cancel, or query flow status.
 *
 * <b>Async lifecycle:</b> Downstream receives a Kafka notification (with {@code approveUrl},
 * {@code cancelUrl}, etc.) at each gate. It calls these endpoints to advance or cancel the flow.
 */
@Slf4j
@RestController
@RequestMapping("/flows/enigio-instrument")
@RequiredArgsConstructor
@Tag(name = "Flow Control",
        description = """
                Approve, cancel, or inspect Enigio instrument flows.

                **Async lifecycle:**
                1. Downstream starts a flow via `POST /flows/enigio-instrument`
                2. DIS publishes a Kafka notification at each gate (PREPARATION_COMPLETE, SIGNING_COMPLETE)
                3. Each notification includes `approveUrl`, `cancelUrl`, `statusUrl` — downstream follows these links
                4. Downstream calls `POST /approve` to advance, `POST /cancel` to abort, or `GET /approval-status` to inspect

                All IDs (instrumentId, correlationId, traceOriginalId) are included in the notification payload —
                downstream never needs to discover or construct them.""")
public class FlowController {

    private final MongoTemplate mongoTemplate;
    private final FlowTypeRegistry flowTypeRegistry;
    @SuppressWarnings("rawtypes")
    private final KafkaTemplate kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final com.orchestrator.starter.outbox.OutboxEventRepository outboxRepository;
    @Value("${orchestrator.kafka.command-topic:dis.instrument.commands}")
    private String commandTopic;

    @Operation(summary = "Start a new instrument flow",
            description = "Creates and starts a new Enigio instrument flow. Validates all required fields.")
    @ApiResponse(responseCode = "202", description = "Flow started")
    @ApiResponse(responseCode = "400", description = "Validation error")
    @SuppressWarnings("unchecked")
    @PostMapping
    public ResponseEntity<?> startFlow(@Valid @RequestBody EnigioInstrumentEntity flow) {
        try {
            if (flow.getCorrelationId() == null) {
                flow.setCorrelationId(UUID.randomUUID().toString());
            }
            var orch = (FlowOrchestrator<EnigioInstrumentEntity>) flowTypeRegistry.resolve("enigio-instrument").getOrchestrator();
            var started = orch.startFlow(flow);
            return ResponseEntity.accepted().body(Map.of(
                    "id", started.getId(),
                    "correlationId", started.getCorrelationId(),
                    "flowType", "enigio-instrument",
                    "status", started.getStatus().name(),
                    "currentStep", started.getCurrentStep()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new ErrorResponse(e.getMessage(), null));
        }
    }

    @Operation(
            summary = "Approve the next phase of a flow",
            description = """
                    Advances the flow past the current approval gate.

                    **When to call:** After receiving a Kafka notification with phase `PREPARATION_COMPLETE`
                    or `SIGNING_COMPLETE` and status `AWAITING_APPROVAL`. The notification's `approveUrl`
                    points directly to this endpoint.

                    **Gate 1 (PREPARATION_COMPLETE):** Document is created, amended, and validated.
                    Approving starts the signing ceremony (emails sent to signers).

                    **Gate 2 (SIGNING_COMPLETE):** All signers have signed.
                    Approving starts envelope creation and delivery to recipient.

                    The flow advances on the next Kafka retry cycle (typically within seconds).""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Approval accepted — flow will advance on next retry cycle",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "instrumentId": "682b3f1a0000000000000001",
                                              "approvedPhase": "signing",
                                              "phase": "awaiting_signing_approval",
                                              "message": "Next phase will start on next retry cycle"
                                            }"""))),
                    @ApiResponse(responseCode = "400", description = "Flow is not at an approval gate",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "error": "Flow is not awaiting approval",
                                              "phase": "signing",
                                              "instrumentId": "682b3f1a0000000000000001"
                                            }"""))),
                    @ApiResponse(responseCode = "404", description = "Flow not found")
            })
    @PostMapping("/{id}/approve")
    public ResponseEntity<?> approveFlow(
            @Parameter(description = "Instrument ID (from notification payload's `instrumentId` field)", example = "682b3f1a0000000000000001")
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> body,
            jakarta.servlet.http.HttpServletRequest request) {

        // Atomic CAS: approve only if flow is still at the expected gate step
        // Prevents TOCTOU race where flow advances between read and update
        String approvedPhase;
        String currentStep;
        String correlationId;

        // Identify caller: forwarded IP (Kubernetes/proxy) + auth principal
        String forwardedFor = request.getHeader("X-Forwarded-For");
        String callerIp = forwardedFor != null ? forwardedFor.split(",")[0].trim() : request.getRemoteAddr();
        var auth = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        String approvedBy = (auth != null ? auth.getName() : "unknown") + "@" + callerIp;
        java.time.Instant now = java.time.Instant.now();

        // Try signing approval gate
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                Query.query(Criteria.where("_id").is(id)
                        .and("currentStep").is(FlowStep.AWAIT_PREPARATION_APPROVAL.name())),
                new Update().set("signingApproved", true)
                        .set("signingApprovedAt", now)
                        .set("signingApprovedBy", approvedBy),
                org.springframework.data.mongodb.core.FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow != null) {
            approvedPhase = "signing";
            currentStep = flow.getCurrentStep();
            correlationId = flow.getCorrelationId();
            log.info("[{}] Signing phase approved by downstream", id);
        } else {
            // Try delivery approval gate
            flow = mongoTemplate.findAndModify(
                    Query.query(Criteria.where("_id").is(id)
                            .and("currentStep").is(FlowStep.AWAIT_DELIVERY_APPROVAL.name())),
                    new Update().set("deliveryApproved", true)
                            .set("deliveryApprovedAt", now)
                            .set("deliveryApprovedBy", approvedBy),
                    org.springframework.data.mongodb.core.FindAndModifyOptions.options().returnNew(true),
                    EnigioInstrumentEntity.class);

            if (flow != null) {
                approvedPhase = "delivery";
                currentStep = flow.getCurrentStep();
                correlationId = flow.getCorrelationId();
                log.info("[{}] Delivery phase approved by downstream", id);
            } else {
                // Neither gate matched — flow may have advanced or doesn't exist
                EnigioInstrumentEntity existing = mongoTemplate.findById(id, EnigioInstrumentEntity.class);
                if (existing == null) return ResponseEntity.notFound().build();
                return ResponseEntity.badRequest().body(new ErrorResponse(
                        "Flow is not awaiting approval (current step may have changed)", id,
                        existing.getCurrentStep() != null ? stepToPhase(existing.getCurrentStep()) : "unknown", null));
            }
        }

        // Re-activate: publish step command to Kafka
        reactivateFlow(id, correlationId, currentStep);

        return ResponseEntity.ok(new ApprovalResponse(
                id, approvedPhase, stepToPhase(currentStep),
                "Flow re-activated — next group starting"));
    }

    @Operation(
            summary = "Get current approval status",
            description = """
                    Returns the flow's current step, overall status, and approval flags for each gate.
                    Use this to check whether a notification was already acted on, or to poll
                    approval state before deciding whether to approve or cancel.

                    The `approvalStatusUrl` in the notification payload points to this endpoint.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Approval status returned",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "instrumentId": "682b3f1a0000000000000001",
                                              "phase": "awaiting_signing_approval",
                                              "status": "IN_PROGRESS",
                                              "preparationNotified": true,
                                              "signingApproved": false,
                                              "signingNotified": false,
                                              "deliveryApproved": false
                                            }"""))),
                    @ApiResponse(responseCode = "404", description = "Flow not found")
            })
    @GetMapping("/{id}/approval-status")
    public ResponseEntity<?> getApprovalStatus(
            @Parameter(description = "Instrument ID (from notification payload's `instrumentId` field)", example = "682b3f1a0000000000000001")
            @PathVariable String id) {
        EnigioInstrumentEntity flow = mongoTemplate.findById(id, EnigioInstrumentEntity.class);
        if (flow == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(new ApprovalStatusResponse(
                id,
                flow.getCurrentStep() != null ? stepToPhase(flow.getCurrentStep()) : "",
                flow.getStatus().name(),
                flow.isPreparationNotified(),
                flow.isSigningApproved(),
                flow.isSigningNotified(),
                flow.isDeliveryApproved()));
    }

    @Operation(
            summary = "Cancel a running flow",
            description = """
                    Cancels the flow and runs compensation handlers in reverse order:
                    1. Cancel envelope transfer (if recipient hasn't opened yet)
                    2. Invalidate sealed envelope (if created)
                    3. Invalidate document on Enigio ledger (permanent — document reaches end state)

                    **Cancellation is allowed when:** flow is IN_PROGRESS, WAITING_RETRY, or PENDING.

                    **Cancellation is NOT allowed when:**
                    - Flow is COMPLETED (document already transferred and potentially opened)
                    - Flow is already CANCELLED or FAILED
                    - Document is inTransit on Enigio (recipient may have opened the envelope)

                    A `FLOW_CANCELLED` notification is published to the notification topic.
                    The `cancelUrl` in the notification payload points to this endpoint.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Flow cancelled — compensation handlers executed",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "instrumentId": "682b3f1a0000000000000001",
                                              "status": "CANCELLED",
                                              "message": "Flow cancelled. user requested"
                                            }"""))),
                    @ApiResponse(responseCode = "400", description = "Flow is not in a cancellable state",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {
                                              "error": "Cannot cancel — flow not in cancellable state (must be IN_PROGRESS, WAITING_RETRY, PARKED, or PENDING)",
                                              "instrumentId": "682b3f1a0000000000000001"
                                            }"""))),
                    @ApiResponse(responseCode = "500", description = "Compensation handler failed")
            })
    @PostMapping("/{id}/cancel")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ResponseEntity<?> cancelFlow(
            @Parameter(description = "Instrument ID (from notification payload's `instrumentId` field)", example = "682b3f1a0000000000000001")
            @PathVariable String id,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Optional cancellation reason",
                    content = @Content(mediaType = "application/json",
                            examples = @ExampleObject(value = """
                                    {"reason": "Compliance review failed — voiding instrument"}""")))
            @RequestBody(required = false) Map<String, Object> body) {

        String reason = body != null ? (String) body.getOrDefault("reason", "user requested") : "user requested";

        try {
            FlowOrchestrator orch = (FlowOrchestrator) flowTypeRegistry.resolve("enigio-instrument").getOrchestrator();
            OrchestratorFlow cancelled = orch.cancelFlow(id, reason);

            if (cancelled == null) {
                return ResponseEntity.badRequest().body(new ErrorResponse(
                        "Cannot cancel — flow not in cancellable state (must be IN_PROGRESS, WAITING_RETRY, PARKED, or PENDING)", id));
            }

            return ResponseEntity.ok(new CancelResponse(
                    cancelled.getId(), cancelled.getStatus().name(),
                    "Flow cancelled. " + cancelled.getErrorMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(new ErrorResponse(e.getMessage(), id));
        }
    }

    /** Re-activate a parked flow by publishing a step command to Kafka, with outbox fallback. */
    @SuppressWarnings("unchecked")
    private void reactivateFlow(String flowId, String correlationId, String stepName) {
        String partitionKey = correlationId != null ? correlationId : flowId;
        try {
            StepCommandMessage cmd = StepCommandMessage.builder()
                    .eventId(UUID.randomUUID().toString())
                    .flowId(flowId)
                    .correlationId(correlationId)
                    .stepName(stepName)
                    .flowType("enigio-instrument")
                    .build();
            kafkaTemplate.send(commandTopic, partitionKey, objectMapper.writeValueAsString(cmd)).get();
            log.info("[{}] Re-activated flow at step {} via Kafka", flowId, stepName);
        } catch (Exception e) {
            // Outbox fallback — outbox publisher will retry when Kafka is available
            log.warn("[{}] Kafka re-activation failed, writing outbox fallback: {}", flowId, e.getMessage());
            try {
                StepCommandMessage cmd = StepCommandMessage.builder()
                        .eventId(UUID.randomUUID().toString())
                        .flowId(flowId)
                        .correlationId(correlationId)
                        .stepName(stepName)
                        .flowType("enigio-instrument")
                        .build();
                outboxRepository.save(com.orchestrator.starter.outbox.OutboxEvent.builder()
                        .id(UUID.randomUUID().toString())
                        .flowId(flowId)
                        .topic(commandTopic)
                        .key(partitionKey)
                        .payload(objectMapper.writeValueAsString(cmd))
                        .build());
            } catch (Exception ex) {
                log.error("[{}] Outbox fallback also failed: {}", flowId, ex.getMessage());
            }
        }
    }

    /** Maps internal step names to downstream-friendly phase names. */
    private static String stepToPhase(String step) {
        try {
            return FlowStep.valueOf(step).phase();
        } catch (IllegalArgumentException e) {
            return step.toLowerCase();
        }
    }
}
