package com.enigio.orchestrator.mock.service;

import com.enigio.orchestrator.mock.config.FailureConfig;
import com.enigio.orchestrator.mock.model.FailureScenario;
import com.enigio.orchestrator.mock.model.MockDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class MockDocumentService {

    private final FailureConfig failureConfig;
    private final Map<String, MockDocument> documents = new ConcurrentHashMap<>();

    public MockDocument createDocument(String title, String content, String metadata) {
        applyFailure("createDocument");
        simulateDelay(200, 500);

        MockDocument doc = MockDocument.builder()
                .documentId(UUID.randomUUID().toString())
                .title(title)
                .content(content)
                .metadata(metadata)
                .build();
        documents.put(doc.getDocumentId(), doc);
        log.info("Created document: {}", doc.getDocumentId());
        return doc;
    }

    public String uploadAttachment(String documentId, String fileName, String fileContent) {
        applyFailure("uploadAttachment");
        simulateDelay(300, 600);

        MockDocument doc = getDocument(documentId);
        String attachmentId = UUID.randomUUID().toString();
        doc.setAttachmentId(attachmentId);
        log.info("Uploaded attachment {} for document {}", attachmentId, documentId);
        return attachmentId;
    }

    public String requestSignature(String documentId, String signerEmail) {
        applyFailure("requestSignature");
        simulateDelay(200, 400);

        MockDocument doc = getDocument(documentId);
        String signatureRequestId = UUID.randomUUID().toString();
        doc.setSignatureRequestId(signatureRequestId);
        log.info("Signature requested for document {} by {}", documentId, signerEmail);
        return signatureRequestId;
    }

    public boolean verifySignature(String documentId, String signatureRequestId) {
        applyFailure("verifySignature");
        simulateDelay(100, 300);

        MockDocument doc = getDocument(documentId);
        doc.setVerifyCallCount(doc.getVerifyCallCount() + 1);

        // Returns PENDING first 2 calls, then VERIFIED
        boolean verified = doc.getVerifyCallCount() > 2;
        log.info("Verify signature for document {}: verified={} (call #{})",
                documentId, verified, doc.getVerifyCallCount());
        return verified;
    }

    public MockDocument finalizeDocument(String documentId) {
        applyFailure("finalizeDocument");
        simulateDelay(400, 800);

        MockDocument doc = getDocument(documentId);
        doc.setFinalized(true);
        log.info("Finalized document: {}", documentId);
        return doc;
    }

    public void resetAll() {
        documents.clear();
        failureConfig.reset();
    }

    private MockDocument getDocument(String documentId) {
        MockDocument doc = documents.get(documentId);
        if (doc == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Document not found: " + documentId);
        }
        return doc;
    }

    private void applyFailure(String endpoint) {
        FailureScenario scenario = failureConfig.getFailureFor(endpoint);
        switch (scenario) {
            case TIMEOUT -> {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            case HTTP_500 -> throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Simulated 500 error for " + endpoint);
            case HTTP_429 -> throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "Simulated 429 rate limit for " + endpoint);
            case FLAKY -> {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                            "Simulated flaky error for " + endpoint);
                }
            }
            case NONE -> { }
        }
    }

    private void simulateDelay(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
