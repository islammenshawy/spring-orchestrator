package com.docuseal.signing.client;

import com.docuseal.signing.model.SigningFlowEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Business logic for document signing operations.
 * The flow definition orchestrates; this service does the work.
 */
@Slf4j
@Service
public class SigningService {

    private final DocuSealClient docuSealClient;
    private final EmailService emailService;

    @Value("${docuseal.template-id}")
    private int templateId;

    public SigningService(DocuSealClient docuSealClient, EmailService emailService) {
        this.docuSealClient = docuSealClient;
        this.emailService = emailService;
    }

    /** Create a submission on DocuSeal with two parties (sequential signing). */
    @SuppressWarnings("unchecked")
    public void createSubmission(SigningFlowEntity flow) {
        log.info("[{}] Creating DocuSeal submission for '{}'", flow.getId(), flow.getReference());

        List<Map<String, Object>> submitters = docuSealClient.createSubmission(
                templateId,
                flow.getPartyAEmail(), flow.getPartyAName(),
                flow.getPartyBEmail(), flow.getPartyBName());

        for (Map<String, Object> submitter : submitters) {
            String role = (String) submitter.get("role");
            int submitterId = ((Number) submitter.get("id")).intValue();
            int submissionId = ((Number) submitter.get("submission_id")).intValue();

            flow.setSubmissionId(submissionId);

            if ("Party A".equals(role)) {
                flow.setPartyASubmitterId(submitterId);
                flow.setPartyAStatus((String) submitter.get("status"));
            } else if ("Party B".equals(role)) {
                flow.setPartyBSubmitterId(submitterId);
                flow.setPartyBStatus((String) submitter.get("status"));
            }
        }

        log.info("[{}] Submission {} created", flow.getId(), flow.getSubmissionId());
    }

    /**
     * Refresh signing status for a specific submitter from DocuSeal.
     * Returns true if the submitter has completed signing.
     */
    @SuppressWarnings("unchecked")
    public boolean refreshSubmitterStatus(SigningFlowEntity flow, int submitterId, boolean isPartyA) {
        Map<String, Object> submission = docuSealClient.getSubmission(flow.getSubmissionId());
        List<Map<String, Object>> submitters = (List<Map<String, Object>>) submission.get("submitters");

        for (Map<String, Object> submitter : submitters) {
            if (((Number) submitter.get("id")).intValue() == submitterId) {
                String status = (String) submitter.get("status");

                if (isPartyA) {
                    flow.setPartyAStatus(status);
                    if ("completed".equals(status)) {
                        extractPartyAValues(flow, submitter);
                        return true;
                    }
                } else {
                    flow.setPartyBStatus(status);
                    if ("completed".equals(status)) {
                        extractDocumentUrls(flow, submission);
                        return true;
                    }
                }
                break;
            }
        }
        return false;
    }

    /** Pre-fill Party B's fields with Party A's signed values and trigger email. */
    public void enrichPartyB(SigningFlowEntity flow) {
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

        emailService.sendPartyBNotification(
                flow.getPartyBEmail(), flow.getPartyAName(), flow.getReference());

        flow.setPartyBNotified(true);
        log.info("[{}] Party B notified at {}", flow.getId(), flow.getPartyBEmail());
    }

    /** Send completion emails to both parties. */
    public void sendCompletionEmails(SigningFlowEntity flow) {
        emailService.sendCompletionNotification(
                flow.getPartyAEmail(), flow.getReference(), flow.getSignedDocumentUrl());
        emailService.sendCompletionNotification(
                flow.getPartyBEmail(), flow.getReference(), flow.getSignedDocumentUrl());

        flow.setCompletionNotified(true);
        log.info("[{}] Completion emails sent", flow.getId());
    }

    @SuppressWarnings("unchecked")
    private void extractPartyAValues(SigningFlowEntity flow, Map<String, Object> submitter) {
        List<Map<String, Object>> values = (List<Map<String, Object>>) submitter.get("values");
        if (values != null) {
            for (Map<String, Object> v : values) {
                if ("Party A Name".equals(v.get("field"))) {
                    flow.setPartyASignedName((String) v.get("value"));
                }
            }
        }
        log.info("[{}] Party A signed. Name: {}", flow.getId(), flow.getPartyASignedName());
    }

    @SuppressWarnings("unchecked")
    private void extractDocumentUrls(SigningFlowEntity flow, Map<String, Object> submission) {
        flow.setAuditLogUrl((String) submission.get("audit_log_url"));
        List<Map<String, Object>> docs = (List<Map<String, Object>>) submission.get("documents");
        if (docs != null && !docs.isEmpty()) {
            flow.setSignedDocumentUrl((String) docs.get(0).get("url"));
        } else {
            flow.setSignedDocumentUrl((String) submission.get("audit_log_url"));
        }
        log.info("[{}] Document complete. URL: {}", flow.getId(),
                flow.getSignedDocumentUrl() != null ? "available" : "pending");
    }
}
