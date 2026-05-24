package com.docuseal.signing.model;

import com.orchestrator.starter.domain.AbstractFlow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "signing_flows")
public class SigningFlowEntity extends AbstractFlow {

    private String reference;

    // DocuSeal IDs
    private int submissionId;
    private int partyASubmitterId;
    private int partyBSubmitterId;

    // Parties
    private String partyAEmail;
    private String partyBEmail;
    private String partyAName;
    private String partyBName;

    // Signing state
    private String partyAStatus;  // sent, opened, completed, declined
    private String partyBStatus;
    private String partyASignedName;

    // Results
    private String signedDocumentUrl;
    private String auditLogUrl;

    // Email tracking
    private boolean partyANotified;
    private boolean partyBNotified;
    private boolean completionNotified;
}
