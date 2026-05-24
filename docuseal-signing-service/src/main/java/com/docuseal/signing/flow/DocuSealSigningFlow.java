package com.docuseal.signing.flow;

import com.docuseal.signing.client.DocuSealClient;
import com.docuseal.signing.client.EmailService;
import com.docuseal.signing.model.SigningFlowEntity;
import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Two-party document signing flow using DocuSeal API.
 *
 * Flow:
 *   1. CREATE_SUBMISSION  — create signing request on DocuSeal (Party A first)
 *   2. POLL_PARTY_A       — poll until Party A signs (pollUntil)
 *   3. ENRICH_PARTY_B     — pre-fill Party B's fields with Party A's values, trigger email
 *   4. POLL_PARTY_B       — poll until Party B signs (pollUntil)
 *   5. SEND_CONFIRMATION  — email both parties with signed document link
 */
@Slf4j
@Component
@Flow(name = "docuseal-signing", topic = "docuseal.signing.commands")
public class DocuSealSigningFlow extends FlowDefinition<SigningFlowEntity> {

    private final DocuSealClient docuSealClient;
    private final EmailService emailService;

    @Value("${docuseal.template-id}")
    private int templateId;

    @Value("${docuseal.poll-interval-seconds:30}")
    private int pollIntervalSeconds;

    public DocuSealSigningFlow(DocuSealClient docuSealClient, EmailService emailService) {
        this.docuSealClient = docuSealClient;
        this.emailService = emailService;
    }

    @Step(order = 1)
    public void createSubmission(SigningFlowEntity flow) {
        log.info("[{}] Creating DocuSeal submission for '{}'", flow.getId(), flow.getReference());

        List<Map<String, Object>> submitters = docuSealClient.createSubmission(
                templateId,
                flow.getPartyAEmail(), flow.getPartyAName(),
                flow.getPartyBEmail(), flow.getPartyBName());

        // Response is a list of submitter objects
        for (Map<String, Object> submitter : submitters) {
            String role = (String) submitter.get("role");
            int submitterId = ((Number) submitter.get("id")).intValue();
            int submissionId = ((Number) submitter.get("submission_id")).intValue();

            flow.setSubmissionId(submissionId);

            if ("Party A".equals(role)) {
                flow.setPartyASubmitterId(submitterId);
                flow.setPartyAStatus((String) submitter.get("status"));
                log.info("[{}] Party A submitter ID: {}", flow.getId(), submitterId);
            } else if ("Party B".equals(role)) {
                flow.setPartyBSubmitterId(submitterId);
                flow.setPartyBStatus((String) submitter.get("status"));
                log.info("[{}] Party B submitter ID: {}", flow.getId(), submitterId);
            }
        }

        checkpoint(flow);
        log.info("[{}] Submission {} created. Party A email sent to {}",
                flow.getId(), flow.getSubmissionId(), flow.getPartyAEmail());
    }

    @Step(order = 2)
    @SuppressWarnings("unchecked")
    public void pollPartyA(SigningFlowEntity flow) {
        log.info("[{}] Polling Party A signing status (submitter {})",
                flow.getId(), flow.getPartyASubmitterId());

        Map<String, Object> submission = docuSealClient.getSubmission(flow.getSubmissionId());
        List<Map<String, Object>> submitters = (List<Map<String, Object>>) submission.get("submitters");

        for (Map<String, Object> submitter : submitters) {
            if (((Number) submitter.get("id")).intValue() == flow.getPartyASubmitterId()) {
                String status = (String) submitter.get("status");
                flow.setPartyAStatus(status);

                if ("completed".equals(status)) {
                    // Extract Party A's signed values
                    List<Map<String, Object>> values = (List<Map<String, Object>>) submitter.get("values");
                    if (values != null) {
                        for (Map<String, Object> v : values) {
                            if ("Party A Name".equals(v.get("field"))) {
                                flow.setPartyASignedName((String) v.get("value"));
                            }
                        }
                    }
                    checkpoint(flow);
                    log.info("[{}] Party A ({}) signed. Name: {}",
                            flow.getId(), flow.getPartyAEmail(), flow.getPartyASignedName());
                    return;
                }
                break;
            }
        }

        // Not signed yet — poll again
        pollUntil(() -> false,
                Duration.ofSeconds(pollIntervalSeconds),
                Duration.ofHours(48));
    }

    @Step(order = 3)
    public void reviewPeriod(SigningFlowEntity flow) {
        log.info("[{}] Party A signed — 30s review period before notifying Party B", flow.getId());
        sleep(flow, Duration.ofSeconds(30));
        log.info("[{}] Review period complete", flow.getId());
    }

    @Step(order = 4)
    public void enrichPartyB(SigningFlowEntity flow) {
        log.info("[{}] Enriching Party B fields with Party A's values", flow.getId());

        // Pre-fill Party B's readonly field with Party A's signed name
        String agreedName = flow.getPartyASignedName() != null
                ? flow.getPartyASignedName() : flow.getPartyAName();

        docuSealClient.updateSubmitter(flow.getPartyBSubmitterId(), Map.of(
                "values", Map.of("Agreed Party A Name", agreedName),
                "send_email", true,
                "message", Map.of(
                        "subject", flow.getPartyAName() + " has signed — your turn: " + flow.getReference(),
                        "body", flow.getPartyAName() + " has reviewed and signed the document '"
                                + flow.getReference() + "'. Please follow the link below to review and sign.\n\n{{submitter.link}}"
                )
        ));

        // Also send our own notification email
        emailService.sendPartyBNotification(
                flow.getPartyBEmail(), flow.getPartyAName(), flow.getReference());

        flow.setPartyBNotified(true);
        log.info("[{}] Party B notified and signing link sent to {}", flow.getId(), flow.getPartyBEmail());
    }

    @Step(order = 5)
    @SuppressWarnings("unchecked")
    public void pollPartyB(SigningFlowEntity flow) {
        log.info("[{}] Polling Party B signing status (submitter {})",
                flow.getId(), flow.getPartyBSubmitterId());

        Map<String, Object> submission = docuSealClient.getSubmission(flow.getSubmissionId());
        List<Map<String, Object>> submitters = (List<Map<String, Object>>) submission.get("submitters");

        for (Map<String, Object> submitter : submitters) {
            if (((Number) submitter.get("id")).intValue() == flow.getPartyBSubmitterId()) {
                String status = (String) submitter.get("status");
                flow.setPartyBStatus(status);

                if ("completed".equals(status)) {
                    // Grab signed document URL
                    flow.setSignedDocumentUrl((String) submission.get("audit_log_url"));
                    List<Map<String, Object>> docs = (List<Map<String, Object>>) submission.get("documents");
                    if (docs != null && !docs.isEmpty()) {
                        flow.setSignedDocumentUrl((String) docs.get(0).get("url"));
                    }
                    flow.setAuditLogUrl((String) submission.get("audit_log_url"));
                    checkpoint(flow);
                    log.info("[{}] Party B ({}) signed. Document complete.",
                            flow.getId(), flow.getPartyBEmail());
                    return;
                }
                break;
            }
        }

        pollUntil(() -> false,
                Duration.ofSeconds(pollIntervalSeconds),
                Duration.ofHours(48));
    }

    @Step(order = 6)
    public void sendConfirmation(SigningFlowEntity flow) {
        log.info("[{}] Sending completion confirmation to both parties", flow.getId());

        emailService.sendCompletionNotification(
                flow.getPartyAEmail(), flow.getReference(), flow.getSignedDocumentUrl());
        emailService.sendCompletionNotification(
                flow.getPartyBEmail(), flow.getReference(), flow.getSignedDocumentUrl());

        flow.setCompletionNotified(true);
        log.info("[{}] Flow complete. Document signed by both parties.", flow.getId());
    }
}
