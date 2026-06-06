package com.docuseal.signing.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

/**
 * REST client for DocuSeal e-signature API.
 * <a href="https://www.docuseal.com/docs/api">API Docs</a>
 */
@Slf4j
@Component
public class DocuSealClient {

    private final RestClient restClient;

    public DocuSealClient(
            @Value("${docuseal.base-url}") String baseUrl,
            @Value("${docuseal.api-key}") String apiKey) {
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("X-Auth-Token", apiKey)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    /**
     * Create a submission with sequential signing (Party A first, then Party B).
     * Returns the list of submitter objects with their IDs and statuses.
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> createSubmission(int templateId,
                                                       String partyAEmail, String partyAName,
                                                       String partyBEmail, String partyBName) {
        var body = Map.of(
                "template_id", templateId,
                "send_email", true,
                "order", "preserved",
                "submitters", List.of(
                        Map.of(
                                "role", "Party A",
                                "email", partyAEmail,
                                "name", partyAName,
                                "order", 0
                        ),
                        Map.of(
                                "role", "Party B",
                                "email", partyBEmail,
                                "name", partyBName,
                                "order", 1,
                                "send_email", false
                                // Don't auto-email Party B — our enrichPartyB step sends
                                // a custom email after pre-filling Party A's signed values
                        )
                )
        );

        log.info("[DocuSeal] Creating submission for template {} with Party A={} Party B={}",
                templateId, partyAEmail, partyBEmail);

        return restClient.post()
                .uri("/submissions")
                .body(body)
                .retrieve()
                .body(List.class);
    }

    /** Get submission details including submitter statuses and signed values. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getSubmission(int submissionId) {
        return restClient.get()
                .uri("/submissions/{id}", submissionId)
                .retrieve()
                .body(Map.class);
    }

    /**
     * Update a submitter — pre-fill fields, resend email, etc.
     * Used to inject Party A's signed values into Party B's readonly fields.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> updateSubmitter(int submitterId, Map<String, Object> updates) {
        log.info("[DocuSeal] Updating submitter {} with {}", submitterId, updates.keySet());

        return restClient.put()
                .uri("/submitters/{id}", submitterId)
                .body(updates)
                .retrieve()
                .body(Map.class);
    }
}
