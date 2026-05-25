package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.WebhookEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Handles audit/confirmation webhook events that only set flags or log.
 * Covers: CREATE, AMENDMENT, INVALIDATE, TRANSFER_CANCELLED.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AuditEventHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.CREATE, WebhookEvent.AMENDMENT,
                WebhookEvent.INVALIDATE, WebhookEvent.TRANSFER_CANCELLED);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        String eventType = (String) payload.get("eventType");
        Query query = Query.query(Criteria.where("traceOriginalId").is(traceOriginalId));

        switch (WebhookEvent.valueOf(eventType)) {
            case CREATE -> {
                mongoTemplate.updateFirst(query,
                        new Update().set("vendorCreateConfirmed", true),
                        EnigioInstrumentEntity.class);
                log.info("[webhook] CREATE confirmed for traceOriginalId={}", traceOriginalId);
            }
            case AMENDMENT -> {
                mongoTemplate.updateFirst(query,
                        new Update().set("vendorAmendConfirmed", true),
                        EnigioInstrumentEntity.class);
                log.info("[webhook] AMENDMENT confirmed for traceOriginalId={}", traceOriginalId);
            }
            case INVALIDATE ->
                log.info("[webhook] INVALIDATE confirmed for traceOriginalId={}", traceOriginalId);
            case TRANSFER_CANCELLED ->
                log.info("[webhook] TRANSFER_CANCELLED confirmed for traceOriginalId={}", traceOriginalId);
            default -> log.info("[webhook] Unhandled audit event: {} for {}", eventType, traceOriginalId);
        }
    }
}
