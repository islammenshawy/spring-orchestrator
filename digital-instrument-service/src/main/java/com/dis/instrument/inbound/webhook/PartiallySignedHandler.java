package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.FlowPhase;
import com.dis.instrument.model.FlowStep;
import com.dis.instrument.model.SigningStatus;
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

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartiallySignedHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;
    private final NotificationService notificationPublisher;
    private final FlowReactivator flowReactivator;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.PARTIALLY_SIGNED);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        // Atomic guard + increment: only increment if below required count
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                Query.query(Criteria.where("traceOriginalId").is(traceOriginalId)
                        .and("$expr").is(new org.bson.Document("$lt",
                                List.of("$signaturesReceived", "$signaturesRequired")))),
                new Update()
                        .inc("signaturesReceived", 1)
                        .set("signingStatus", SigningStatus.PARTIALLY_SIGNED.name()),
                FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow == null) {
            log.info("[webhook] PARTIALLY_SIGNED ignored — already fully signed or unknown doc");
            return;
        }

        log.info("[webhook] Signature {}/{} received for instrument {}",
                flow.getSignaturesReceived(), flow.getSignaturesRequired(), flow.getId());

        notificationPublisher.notifyPhaseComplete(flow,
                FlowPhase.SIGNATURE_RECEIVED.name(),
                flow.getSignaturesReceived() + "/" + flow.getSignaturesRequired() + " signed");

        if (flow.getSignaturesRequired() > 0
                && flow.getSignaturesReceived() >= flow.getSignaturesRequired()) {
            Query query = Query.query(Criteria.where("traceOriginalId").is(traceOriginalId));
            mongoTemplate.updateFirst(query,
                    new Update().set("signingStatus", SigningStatus.SIGNED.name()),
                    EnigioInstrumentEntity.class);
            log.info("[webhook] All {} signatures received — marking SIGNED", flow.getSignaturesRequired());

            notificationPublisher.notifyPhaseComplete(flow,
                    FlowPhase.ALL_SIGNATURES_COMPLETE.name(), "SIGNED");

            flowReactivator.reactivate(flow.getId(), FlowStep.AWAIT_SIGNATURES.name());
        }
    }
}
