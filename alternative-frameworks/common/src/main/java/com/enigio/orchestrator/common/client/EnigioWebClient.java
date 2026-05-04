package com.enigio.orchestrator.common.client;

import com.enigio.orchestrator.common.client.dto.*;
import com.enigio.orchestrator.common.exception.NonRetryableException;
import com.enigio.orchestrator.common.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;

@Slf4j
@Component
public class EnigioWebClient implements EnigioClient {

    private final WebClient webClient;

    public EnigioWebClient(
            @Value("${enigio.client.base-url}") String baseUrl,
            @Value("${enigio.client.read-timeout:10000}") int readTimeout) {
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    @Override
    public CreateDocumentResponse createDocument(CreateDocumentRequest request) {
        return post("/documents", request, CreateDocumentResponse.class);
    }

    @Override
    public UploadAttachmentResponse uploadAttachment(String documentId, UploadAttachmentRequest request) {
        return post("/documents/" + documentId + "/attachments", request, UploadAttachmentResponse.class);
    }

    @Override
    public RequestSignatureResponse requestSignature(String documentId, RequestSignatureRequest request) {
        return post("/documents/" + documentId + "/sign", request, RequestSignatureResponse.class);
    }

    @Override
    public VerifySignatureResponse verifySignature(String documentId, String signatureRequestId) {
        return webClient.get()
                .uri("/documents/{id}/signature-status?signatureRequestId={sid}", documentId, signatureRequestId)
                .retrieve()
                .bodyToMono(VerifySignatureResponse.class)
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> log.error("Error verifying signature for document {}: {}", documentId, e.getMessage()))
                .onErrorMap(this::mapException)
                .block();
    }

    @Override
    public FinalizeDocumentResponse finalizeDocument(String documentId) {
        return post("/documents/" + documentId + "/finalize", null, FinalizeDocumentResponse.class);
    }

    private <T> T post(String uri, Object body, Class<T> responseType) {
        WebClient.RequestBodySpec spec = webClient.post().uri(uri);
        if (body != null) {
            return spec.bodyValue(body)
                    .retrieve()
                    .bodyToMono(responseType)
                    .timeout(Duration.ofSeconds(10))
                    .onErrorMap(this::mapException)
                    .block();
        }
        return spec.retrieve()
                .bodyToMono(responseType)
                .timeout(Duration.ofSeconds(10))
                .onErrorMap(this::mapException)
                .block();
    }

    private Throwable mapException(Throwable ex) {
        if (ex instanceof WebClientResponseException wcre) {
            HttpStatusCode status = wcre.getStatusCode();
            if (status.value() == 429 || status.is5xxServerError()) {
                return new RetryableException("Retryable error from Enigio: " + status, ex);
            }
            return new NonRetryableException("Non-retryable error from Enigio: " + status, ex);
        }
        if (ex instanceof java.util.concurrent.TimeoutException) {
            return new RetryableException("Timeout calling Enigio", ex);
        }
        return new RetryableException("Unexpected error calling Enigio", ex);
    }
}
