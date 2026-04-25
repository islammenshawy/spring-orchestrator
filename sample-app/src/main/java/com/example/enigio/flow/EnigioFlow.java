package com.example.enigio.flow;

import com.orchestrator.starter.domain.AbstractFlow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Only your domain fields. All orchestrator tracking fields
 * (id, status, retryCount, currentStep, etc.) are inherited from AbstractFlow.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "enigio_flows")
public class EnigioFlow extends AbstractFlow {

    private String title;
    private String content;
    private String signerEmail;

    private String enigioDocumentId;
    private String attachmentId;
    private String signatureRequestId;
    private String finalDocumentUrl;
    private String traceHash;
}
