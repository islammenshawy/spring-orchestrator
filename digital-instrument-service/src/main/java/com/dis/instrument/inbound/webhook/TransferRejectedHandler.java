package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.FlowStep;
import com.dis.instrument.model.WebhookEvent;
import com.dis.instrument.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransferRejectedHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;
    private final NotificationService notificationPublisher;
    private final FlowReactivator flowReactivator;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.TRANSFER_REJECTED);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        Query envelopeQuery = Query.query(Criteria.where("envelopeTraceId").is(traceOriginalId));
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(envelopeQuery,
                new Update().set("transferRejected", true),
                FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow != null) {
            log.error("[webhook] TRANSFER_REJECTED for instrument {} (envelope={})",
                    flow.getId(), traceOriginalId);
            notificationPublisher.notifyPhaseComplete(flow,
                    "TRANSFER_REJECTED", "REJECTED");
            flowReactivator.reactivate(flow.getId(), FlowStep.TRANSFER_DOCUMENT.name());
        } else {
            log.warn("[webhook] TRANSFER_REJECTED for unknown envelope {}", traceOriginalId);
        }
    }
}
