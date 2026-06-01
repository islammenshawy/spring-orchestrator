package com.docuseal.signing.flow;

import com.docuseal.signing.client.SigningService;
import com.docuseal.signing.model.SigningFlowEntity;
import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.RecoverAction;
import com.orchestrator.starter.annotation.RecoverOn;
import com.orchestrator.starter.annotation.Signal;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Two-party document signing flow — thin orchestration layer.
 * Business logic lives in {@link SigningService}.
 *
 * Flow:
 *   1. CREATE_SUBMISSION  — create signing request on DocuSeal
 *   2. POLL_PARTY_A       — poll until Party A signs
 *   3. REVIEW_PERIOD      — 30s durable sleep
 *   4. ENRICH_PARTY_B     — pre-fill fields, send email
 *   5. POLL_PARTY_B       — poll until Party B signs
 *   6. SEND_CONFIRMATION  — email both parties
 */
@Slf4j
@Component
@Flow(name = "docuseal-signing", topic = "docuseal.signing.commands")
public class DocuSealSigningFlow extends FlowDefinition<SigningFlowEntity> {

    private final SigningService signingService;

    @Value("${docuseal.poll-interval-seconds:30}")
    private int pollIntervalSeconds;

    public DocuSealSigningFlow(SigningService signingService) {
        this.signingService = signingService;
    }

    @Step(order = 1)
    public void createSubmission(SigningFlowEntity flow) {
        signingService.createSubmission(flow);
        checkpoint(flow);
    }

    @Step(order = 2)
    public void pollPartyA(SigningFlowEntity flow) {
        boolean signed = signingService.refreshSubmitterStatus(
                flow, flow.getPartyASubmitterId(), true);
        if (signed) {
            checkpoint(flow);
            return;
        }
        pollUntil(() -> false,
                Duration.ofSeconds(pollIntervalSeconds),
                Duration.ofHours(48));
    }

    @Step(order = 3)
    public void reviewPeriod(SigningFlowEntity flow) {
        sleep(flow, Duration.ofSeconds(30));
    }

    @Step(order = 4)
    public void enrichPartyB(SigningFlowEntity flow) {
        try {
            signingService.enrichPartyB(flow);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("already completed")) {
                log.info("[{}] Party B already signed — skipping enrichment", flow.getId());
                return; // Skip — Party B signed before we could enrich
            }
            throw e;
        }
    }

    @Step(order = 5)
    public void pollPartyB(SigningFlowEntity flow) {
        boolean signed = signingService.refreshSubmitterStatus(
                flow, flow.getPartyBSubmitterId(), false);
        if (signed) {
            checkpoint(flow);
            return;
        }
        pollUntil(() -> false,
                Duration.ofSeconds(pollIntervalSeconds),
                Duration.ofHours(48));
    }

    @Step(order = 6)
    public void sendConfirmation(SigningFlowEntity flow) {
        signingService.sendCompletionEmails(flow);
    }

    // ========== Signals ==========

    @Signal
    public void cancelSigning(SigningFlowEntity flow) {
        if ("completed".equals(flow.getPartyAStatus()) && "completed".equals(flow.getPartyBStatus())) {
            throw new IllegalStateException("Cannot cancel — both parties already signed");
        }
        cancelFlow(flow, "Signing cancelled via signal");
    }
}
