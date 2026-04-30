package com.dis.instrument.vendor.enigio;

import com.dis.instrument.core.model.Attachment;
import com.dis.instrument.core.model.Signer;
import io.netty.channel.ChannelOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HTTP client for Enigio trace:original Fullnode API v3.3.
 * Calls the real (or mock) API endpoints with typed request/response.
 */
@Slf4j
@Component
public class EnigioClient {

    private final WebClient client;

    public EnigioClient(@Value("${vendor.enigio.base-url}") String baseUrl) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
                .responseTimeout(Duration.ofSeconds(10));

        this.client = WebClient.builder()
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    // ===== Documents =====

    public record DocumentResponse(String traceOriginalId, String versionKey) {}

    public DocumentResponse createDocument(String reference, String documentType,
                                           String documentCode, Map<String, Object> content,
                                           Map<String, Object> customData) {
        var body = new java.util.HashMap<String, Object>();
        body.put("reference", reference);
        body.put("documentType", documentType);
        body.put("documentCode", documentCode);
        body.put("content", content);
        if (customData != null) body.put("customData", customData);

        var res = client.post().uri("/documents")
                .bodyValue(body)
                .retrieve().bodyToMono(Map.class).block();

        return new DocumentResponse(
                (String) res.get("traceOriginalId"),
                (String) res.get("versionKey"));
    }

    public DocumentResponse amendDocument(String traceOriginalId, String versionKey,
                                          Map<String, Object> content,
                                          List<Attachment> attachments) {
        var body = new java.util.HashMap<String, Object>();
        body.put("versionKey", versionKey);
        body.put("content", content);
        if (attachments != null && !attachments.isEmpty()) {
            body.put("attachments", attachments.stream()
                    .map(a -> Map.of("filename", a.getFilename(), "data", a.getData(),
                            "comment", a.getComment() != null ? a.getComment() : ""))
                    .collect(Collectors.toList()));
        }

        var res = client.post().uri("/documents/{id}/amend", traceOriginalId)
                .bodyValue(body)
                .retrieve().bodyToMono(Map.class).block();

        return new DocumentResponse(
                (String) res.get("traceOriginalId"),
                (String) res.get("versionKey"));
    }

    public String validateDocument(String traceOriginalId) {
        var res = client.post().uri("/documents/validate")
                .bodyValue(Map.of("document", traceOriginalId))
                .retrieve().bodyToMono(Map.class).block();

        return (String) res.get("result");
    }

    // ===== Required Signatures =====

    public void addRequiredSignatures(String traceOriginalId, List<Signer> signers) {
        var body = signers.stream()
                .map(s -> {
                    var signer = new java.util.HashMap<String, Object>();
                    signer.put("capacityOfSignatory", s.getCapacity());
                    signer.put("organisation", s.getOrganisation());
                    signer.put("signers", List.of(Map.of(
                            "email", s.getEmail(),
                            "phone", s.getPhone(),
                            "name", s.getName()
                    )));
                    return signer;
                })
                .collect(Collectors.toList());

        client.post().uri("/required-signatures/original/{id}", traceOriginalId)
                .bodyValue(body)
                .retrieve().bodyToMono(Void.class).block();
    }

    @SuppressWarnings("unchecked")
    public List<String> sendSigningEmails(String traceOriginalId, String locale) {
        return client.post().uri("/required-signatures/send-sign-emails")
                .bodyValue(Map.of("traceOriginalId", traceOriginalId, "locale", locale))
                .retrieve().bodyToMono(List.class).block();
    }

    public String getSigningStatus(String traceOriginalId) {
        String status = client.get()
                .uri("/required-signatures/original/{id}/status", traceOriginalId)
                .retrieve().bodyToMono(String.class).block();

        // API returns quoted string like "SIGNED"
        return status != null ? status.replace("\"", "") : "PENDING";
    }

    /**
     * Register a webhook on Enigio for signature events.
     * Enigio will POST to our callback URL when signing status changes.
     */
    public void registerWebhook(String callbackUrl, List<String> events) {
        try {
            client.post().uri("/notifications/webhooks")
                    .bodyValue(Map.of(
                            "url", callbackUrl,
                            "events", events,
                            "signatureMethod", "NONE"
                    ))
                    .retrieve().bodyToMono(Map.class).block();
            log.info("Webhook registered: {} for events {}", callbackUrl, events);
        } catch (Exception e) {
            // Webhook registration is best-effort — polling is the fallback
            log.warn("Failed to register webhook (polling fallback active): {}", e.getMessage());
        }
    }

    // ===== Envelopes =====

    public String createEnvelopeDraft(String reference, String coverMessage,
                                      String originalDocumentTraceId) {
        var res = client.post().uri("/envelopes/drafts")
                .bodyValue(Map.of(
                        "reference", reference,
                        "coverMessage", coverMessage,
                        "originalDocuments", List.of(
                                Map.of("traceOriginalId", originalDocumentTraceId))
                ))
                .retrieve().bodyToMono(Map.class).block();

        return (String) res.get("draftId");
    }

    public DocumentResponse sealEnvelopeDraft(String draftId) {
        var res = client.post().uri("/envelopes/drafts/{id}/seal", draftId)
                .retrieve().bodyToMono(Map.class).block();

        return new DocumentResponse(
                (String) res.get("traceOriginalId"),
                (String) res.get("versionKey"));
    }

    public String transferByEmail(String envelopeTraceId, String versionKey,
                                  String recipientEmail, String recipientName,
                                  String comment, String emailMessage) {
        var res = client.post().uri("/envelopes/{id}/transfer-by-email", envelopeTraceId)
                .bodyValue(Map.of(
                        "recipientEmail", recipientEmail,
                        "recipientName", recipientName,
                        "transferComment", comment,
                        "transferEmailMessage", emailMessage,
                        "versionKey", versionKey
                ))
                .retrieve().bodyToMono(Map.class).block();

        return (String) res.get("transferId");
    }
}
