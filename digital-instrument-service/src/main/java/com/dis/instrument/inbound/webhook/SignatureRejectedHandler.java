package com.dis.instrument.inbound.webhook;

import com.dis.instrument.flow.EnigioInstrumentEntity;
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
        // Only reject if not already SIGNED — prevents overwriting a completed signing
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                Query.query(Criteria.where("traceOriginalId").is(traceOriginalId)
                        .and("signingStatus").ne(SigningStatus.SIGNED.name())),
                new Update().set("signingStatus", SigningStatus.REJECTED.name()),
                FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow != null) {
            log.info("[webhook] Signature REJECTED for instrument {}", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    "SIGNATURE_REJECTED", "REJECTED");
        } else {
            log.info("[webhook] SIGNATURE_REJECTED ignored — already SIGNED or unknown doc");
        }
    }
}
