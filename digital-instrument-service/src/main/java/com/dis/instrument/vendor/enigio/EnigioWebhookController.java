package com.dis.instrument.vendor.enigio;

import com.dis.instrument.core.api.FlowNotificationPublisher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Receives webhook callbacks from Enigio trace:original API.
 *
 * <b>Not called by downstream</b> — this endpoint is registered with the Enigio vendor
 * during the signing ceremony. Enigio POSTs here when signing status changes.
 * DIS translates these into Kafka notifications for downstream.
 */
@Slf4j
@RestController
@RequestMapping("/webhooks/enigio")
@RequiredArgsConstructor
@Tag(name = "Enigio Webhooks",
        description = """
                Callback endpoint for Enigio trace:original signing events.

                **Not called by downstream** — this is registered with the Enigio API during
                the signing ceremony step. Enigio fires webhooks when signers act:

                - `PARTIALLY_SIGNED` — one signer completed, others pending
                - `FULLY_SIGNED` — all required signers completed
                - `SIGNATURE_REJECTED` — a signer rejected the document

                DIS translates each webhook into a Kafka notification with the instrument's
                `instrumentId`, `correlationId`, and current signing progress, so downstream
                can track real-time signing status without polling.""")
public class EnigioWebhookController {

    private final MongoTemplate mongoTemplate;
    private final FlowNotificationPublisher notificationPublisher;

    @Operation(
            summary = "Handle Enigio signing webhook",
            description = """
                    Receives signing events from the Enigio trace:original API.
                    Updates the instrument's signing status atomically and publishes
                    a Kafka notification to the downstream topic.

                    **Idempotency:** FULLY_SIGNED is only processed once (duplicate detection).
                    PARTIALLY_SIGNED uses atomic increment to handle concurrent webhooks.

                    **Payload format:** Matches Enigio webhook spec v3.3 —
                    see [Enigio docs](https://docs.traceoriginal.com) for the full schema.""",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Webhook processed",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = """
                                            {"status": "received", "eventType": "PARTIALLY_SIGNED"}"""))),
                    @ApiResponse(responseCode = "400", description = "Missing traceOriginalId or eventType")
            })
    @PostMapping
    public ResponseEntity<Map<String, String>> handleWebhook(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Enigio webhook event payload",
                    content = @Content(mediaType = "application/json",
                            examples = {
                                    @ExampleObject(name = "PARTIALLY_SIGNED", value = """
                                            {
                                              "messageId": "019a36a9-b031-7001-8348-2cc71e288d4a",
                                              "traceOriginalId": "1ff401aa69d511402c452c100cfa63afe9bbf029d1b216303f779e09508118f7",
                                              "eventType": "PARTIALLY_SIGNED",
                                              "timestamp": "2024-04-16T19:01:18.591Z"
                                            }"""),
                                    @ExampleObject(name = "FULLY_SIGNED", value = """
                                            {
                                              "messageId": "019a36a9-b031-7001-8348-2cc71e288d4b",
                                              "traceOriginalId": "1ff401aa69d511402c452c100cfa63afe9bbf029d1b216303f779e09508118f7",
                                              "eventType": "FULLY_SIGNED",
                                              "timestamp": "2024-04-16T19:05:22.103Z"
                                            }"""),
                                    @ExampleObject(name = "SIGNATURE_REJECTED", value = """
                                            {
                                              "messageId": "019a36a9-b031-7001-8348-2cc71e288d4c",
                                              "traceOriginalId": "1ff401aa69d511402c452c100cfa63afe9bbf029d1b216303f779e09508118f7",
                                              "eventType": "SIGNATURE_REJECTED",
                                              "timestamp": "2024-04-16T19:03:41.807Z"
                                            }""")
                            }))
            @RequestBody Map<String, Object> payload) {
        String eventType = (String) payload.get("eventType");
        String traceOriginalId = (String) payload.get("traceOriginalId");

        log.info("[webhook] Received {} for traceOriginalId={}", eventType, traceOriginalId);

        if (traceOriginalId == null || eventType == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing traceOriginalId or eventType"));
        }

        Query query = Query.query(Criteria.where("traceOriginalId").is(traceOriginalId));

        switch (eventType) {
            case "PARTIALLY_SIGNED" -> {
                EnigioInstrumentEntity flow = mongoTemplate.findAndModify(query,
                        new Update()
                                .inc("signaturesReceived", 1)
                                .set("signingStatus", "PARTIALLY_SIGNED"),
                        FindAndModifyOptions.options().returnNew(true),
                        EnigioInstrumentEntity.class);

                if (flow != null) {
                    log.info("[webhook] Signature {}/{} received for instrument {}",
                            flow.getSignaturesReceived(), flow.getSignaturesRequired(), flow.getId());

                    notificationPublisher.notifyPhaseComplete(flow,
                            "SIGNATURE_RECEIVED",
                            flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed");

                    if (flow.getSignaturesRequired() > 0
                            && flow.getSignaturesReceived() >= flow.getSignaturesRequired()) {
                        mongoTemplate.updateFirst(query,
                                new Update().set("signingStatus", "SIGNED"),
                                EnigioInstrumentEntity.class);
                        log.info("[webhook] All {} signatures received — marking SIGNED", flow.getSignaturesRequired());

                        notificationPublisher.notifyPhaseComplete(flow,
                                "ALL_SIGNATURES_COMPLETE", "SIGNED");
                    }
                }
            }

            case "FULLY_SIGNED" -> {
                EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                        Query.query(Criteria.where("traceOriginalId").is(traceOriginalId)
                                .and("signingStatus").ne("SIGNED")),
                        new Update().set("signingStatus", "SIGNED"),
                        FindAndModifyOptions.options().returnNew(true),
                        EnigioInstrumentEntity.class);

                if (flow != null) {
                    log.info("[webhook] FULLY_SIGNED for instrument {}", flow.getId());
                    notificationPublisher.notifyPhaseComplete(flow,
                            "ALL_SIGNATURES_COMPLETE", "SIGNED");
                } else {
                    log.info("[webhook] FULLY_SIGNED received but already SIGNED (duplicate)");
                }
            }

            case "SIGNATURE_REJECTED" -> {
                mongoTemplate.updateFirst(query,
                        new Update().set("signingStatus", "REJECTED"),
                        EnigioInstrumentEntity.class);

                EnigioInstrumentEntity flow = mongoTemplate.findOne(query, EnigioInstrumentEntity.class);
                if (flow != null) {
                    log.info("[webhook] Signature REJECTED for instrument {}", flow.getId());
                    notificationPublisher.notifyPhaseComplete(flow,
                            "SIGNATURE_REJECTED", "REJECTED");
                }
            }

            default -> log.info("[webhook] Ignoring event type: {}", eventType);
        }

        return ResponseEntity.ok(Map.of("status", "received", "eventType", eventType));
    }
}
