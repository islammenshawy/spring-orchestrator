package com.dis.instrument.inbound;

import com.dis.instrument.inbound.response.*;
import com.dis.instrument.inbound.webhook.WebhookEventHandler;
import com.dis.instrument.model.WebhookEvent;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
public class WebhookController {

    private final Map<String, WebhookEventHandler> handlerMap;

    public WebhookController(List<WebhookEventHandler> handlers) {
        this.handlerMap = handlers.stream()
                .flatMap(h -> h.getSupportedEvents().stream()
                        .map(event -> Map.entry(event.name(), h)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("[webhook] Registered handlers for events: {}", handlerMap.keySet());
    }

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
    public ResponseEntity<?> handleWebhook(
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
        String messageId = (String) payload.getOrDefault("messageId", "unknown");

        log.info("[webhook] Received {} for traceOriginalId={} messageId={}", eventType, traceOriginalId, messageId);

        if (traceOriginalId == null || eventType == null) {
            return ResponseEntity.badRequest().body(new ErrorResponse("Missing traceOriginalId or eventType"));
        }

        WebhookEventHandler handler = handlerMap.get(eventType);
        if (handler != null) {
            handler.handle(traceOriginalId, payload);
        } else {
            log.info("[webhook] Ignoring unknown event type: {} for {}", eventType, traceOriginalId);
        }

        return ResponseEntity.ok(new WebhookResponse("received", eventType));
    }
}
