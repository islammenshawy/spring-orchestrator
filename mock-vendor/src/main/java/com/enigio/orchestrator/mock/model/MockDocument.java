package com.enigio.orchestrator.mock.model;

import com.enigio.orchestrator.mock.dto.RequiredSignatureDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MockDocument {

    // Identity
    private String traceOriginalId;
    private String versionKey;
    private int version;

    // Creation fields
    private String reference;
    private String documentType;
    private String documentCode; // NEG | TTL | RGS | RGN | AGT
    private String format;       // PDF | YAML
    private Map<String, Object> content;
    private Map<String, Object> customData;

    // Signatures
    @Builder.Default
    private List<RequiredSignatureDTO> requiredSignatures = new ArrayList<>();
    private boolean signingEmailsSent;
    @Builder.Default
    private int signingPollCount = 0;

    // Validation
    private boolean validated;

    // Envelope
    private String envelopeDraftId;
    private String envelopeTraceId;
    private String envelopeVersionKey;
    @Builder.Default
    private List<Map<String, Object>> additionalDocuments = new ArrayList<>();

    // Transfer
    private String transferId;

    // Webhook callback URL — stored per-document for reliable delivery after restart
    private String callbackUrl;

    // State
    private boolean invalidated;
    private boolean inTransit;

    @Builder.Default
    private Instant createdAt = Instant.now();
}
