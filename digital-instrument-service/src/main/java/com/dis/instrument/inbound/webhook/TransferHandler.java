package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.FlowStep;
import com.dis.instrument.model.WebhookEvent;
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
public class TransferHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;
    private final FlowReactivator flowReactivator;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.TRANSFER);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        Query envelopeQuery = Query.query(Criteria.where("envelopeTraceId").is(traceOriginalId));
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(envelopeQuery,
                new Update().set("transferAccepted", true),
                FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow != null) {
            log.info("[webhook] TRANSFER accepted for instrument {} (envelope={})",
                    flow.getId(), traceOriginalId);
            flowReactivator.reactivate(flow.getId(), FlowStep.TRANSFER_DOCUMENT.name());
        } else {
            log.info("[webhook] TRANSFER for unknown envelope {}", traceOriginalId);
        }
    }
}
