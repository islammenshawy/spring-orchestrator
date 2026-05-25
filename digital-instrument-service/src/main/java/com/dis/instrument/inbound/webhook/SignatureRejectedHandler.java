package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.SigningStatus;
import com.dis.instrument.model.WebhookEvent;
import com.dis.instrument.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
public class SignatureRejectedHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;
    private final NotificationService notificationPublisher;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.SIGNATURE_REJECTED);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        Query query = Query.query(Criteria.where("traceOriginalId").is(traceOriginalId));
        mongoTemplate.updateFirst(query,
                new Update().set("signingStatus", SigningStatus.REJECTED.name()),
                EnigioInstrumentEntity.class);

        EnigioInstrumentEntity flow = mongoTemplate.findOne(query, EnigioInstrumentEntity.class);
        if (flow != null) {
            log.info("[webhook] Signature REJECTED for instrument {}", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    "SIGNATURE_REJECTED", "REJECTED");
        }
    }
}
