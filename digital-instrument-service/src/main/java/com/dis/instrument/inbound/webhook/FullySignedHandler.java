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

import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class FullySignedHandler implements WebhookEventHandler {

    private final MongoTemplate mongoTemplate;
    private final NotificationService notificationPublisher;
    private final FlowReactivator flowReactivator;

    @Override
    public Set<WebhookEvent> getSupportedEvents() {
        return Set.of(WebhookEvent.FULLY_SIGNED);
    }

    @Override
    public void handle(String traceOriginalId, Map<String, Object> payload) {
        EnigioInstrumentEntity flow = mongoTemplate.findAndModify(
                Query.query(Criteria.where("traceOriginalId").is(traceOriginalId)
                        .and("signingStatus").ne(SigningStatus.SIGNED.name())),
                new Update().set("signingStatus", SigningStatus.SIGNED.name()),
                FindAndModifyOptions.options().returnNew(true),
                EnigioInstrumentEntity.class);

        if (flow != null) {
            log.info("[webhook] FULLY_SIGNED for instrument {}", flow.getId());
            notificationPublisher.notifyPhaseComplete(flow,
                    FlowPhase.ALL_SIGNATURES_COMPLETE.name(), "SIGNED");
            flowReactivator.reactivate(flow.getId(), FlowStep.AWAIT_SIGNATURES.name());
        } else {
            log.info("[webhook] FULLY_SIGNED received but already SIGNED (duplicate)");
        }
    }
}
