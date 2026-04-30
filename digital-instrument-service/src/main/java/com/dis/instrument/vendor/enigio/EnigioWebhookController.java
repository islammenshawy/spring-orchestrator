package com.dis.instrument.vendor.enigio;

import com.dis.instrument.core.api.FlowNotificationPublisher;
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
 * Enigio fires these events during the signing ceremony:
 *   PARTIALLY_SIGNED — one signer completed (others still pending)
 *   FULLY_SIGNED     — all signers completed
 *   SIGNATURE_REJECTED — a signer rejected
 *
 * Uses findAndModify for atomic increment + read to avoid race conditions
 * when multiple webhooks arrive simultaneously.
 */
@Slf4j
@RestController
@RequestMapping("/webhooks/enigio")
@RequiredArgsConstructor
public class EnigioWebhookController {

    private final MongoTemplate mongoTemplate;
    private final FlowNotificationPublisher notificationPublisher;

    @PostMapping
    public ResponseEntity<Map<String, String>> handleWebhook(@RequestBody Map<String, Object> payload) {
        String eventType = (String) payload.get("eventType");
        String traceOriginalId = (String) payload.get("traceOriginalId");

        log.info("[webhook] Received {} for traceOriginalId={}", eventType, traceOriginalId);

        if (traceOriginalId == null || eventType == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing traceOriginalId or eventType"));
        }

        Query query = Query.query(Criteria.where("traceOriginalId").is(traceOriginalId));

        switch (eventType) {
            case "PARTIALLY_SIGNED" -> {
                // Atomic increment + return updated document
                EnigioInstrumentEntity flow = mongoTemplate.findAndModify(query,
                        new Update()
                                .inc("signaturesReceived", 1)
                                .set("signingStatus", "PARTIALLY_SIGNED"),
                        FindAndModifyOptions.options().returnNew(true),
                        EnigioInstrumentEntity.class);

                if (flow != null) {
                    log.info("[webhook] Signature {}/{} received for flow {}",
                            flow.getSignaturesReceived(), flow.getSignaturesRequired(), flow.getId());

                    notificationPublisher.notifyPhaseComplete(flow,
                            "SIGNATURE_RECEIVED",
                            flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed");

                    // Check if all signers done (this is the only place that sets SIGNED from partials)
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
                // Only set SIGNED if not already set (prevents duplicate notification)
                EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                        Query.query(Criteria.where("traceOriginalId").is(traceOriginalId)
                                .and("signingStatus").ne("SIGNED")),
                        new Update().set("signingStatus", "SIGNED"),
                        FindAndModifyOptions.options().returnNew(true),
                        EnigioInstrumentEntity.class);

                if (flow != null) {
                    log.info("[webhook] FULLY_SIGNED for flow {}", flow.getId());
                    notificationPublisher.notifyPhaseComplete(flow,
                            "ALL_SIGNATURES_COMPLETE", "SIGNED");
                } else {
                    log.info("[webhook] FULLY_SIGNED received but flow already SIGNED (duplicate)");
                }
            }

            case "SIGNATURE_REJECTED" -> {
                mongoTemplate.updateFirst(query,
                        new Update().set("signingStatus", "REJECTED"),
                        EnigioInstrumentEntity.class);

                EnigioInstrumentEntity flow = mongoTemplate.findOne(query, EnigioInstrumentEntity.class);
                if (flow != null) {
                    log.info("[webhook] Signature REJECTED for flow {}", flow.getId());
                    notificationPublisher.notifyPhaseComplete(flow,
                            "SIGNATURE_REJECTED", "REJECTED");
                }
            }

            default -> log.info("[webhook] Ignoring event type: {}", eventType);
        }

        return ResponseEntity.ok(Map.of("status", "received", "eventType", eventType));
    }
}
