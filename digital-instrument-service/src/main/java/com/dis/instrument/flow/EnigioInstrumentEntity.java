package com.dis.instrument.flow;

import com.dis.instrument.model.*;
import com.orchestrator.starter.annotation.SearchAttribute;
import com.orchestrator.starter.domain.AbstractFlow;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

/**
 * Persistent entity for an Enigio instrument flow.
 *
 * Input fields are set by the downstream API request.
 * Result fields are populated by each step during execution.
 * Orchestrator tracking fields (status, currentStep, retryCount, etc.)
 * are inherited from AbstractFlow.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "dis_instrument_flows")
public class EnigioInstrumentEntity extends AbstractFlow {

    // ===== Input — from downstream request =====

    @SearchAttribute
    @NotBlank(message = "reference is required")
    private String reference;

    @NotBlank(message = "title is required")
    private String title;

    private String content; // document body / terms text

    @SearchAttribute
    @NotNull(message = "instrumentType is required")
    private InstrumentType instrumentType;

    @NotNull(message = "documentCode is required")
    private DocumentCode documentCode;

    @Valid
    private List<Party> parties;

    @NotEmpty(message = "at least one signer is required")
    @Valid
    private List<Signer> signers;

    @NotNull(message = "recipient is required")
    @Valid
    private Recipient recipient;

    private List<Attachment> attachments;
    private Map<String, Object> customData;
    private String callbackUrl;
    private String priority;

    /** MongoDB document IDs for additional documents to attach to the envelope.
     *  Only IDs go through Kafka — binary content stays in MongoDB. */
    private List<String> additionalDocumentIds;

    // ===== Group 1 results — Document Preparation =====

    private boolean pdfGenerated;
    private String traceOriginalId;
    private String versionKey;
    private String attachmentVersionKey;

    // ===== Gate: Group 1 → Group 2 =====

    private boolean preparationNotified;   // notification published to topic
    private java.time.Instant preparationNotifiedAt; // for approval expiry
    private boolean signingApproved;       // downstream approved via API

    // ===== Group 2 results — Signing Ceremony =====

    private boolean signersAdded;
    private boolean signingEmailsSent;
    private boolean webhookRegistered;
    private java.time.Instant signingStartedAt;  // when signing emails were sent (for expiry)
    private String signingStatus;       // PENDING | PARTIALLY_SIGNED | SIGNED | REJECTED | EXPIRED
    private int signaturesReceived;     // count of individual PARTIALLY_SIGNED events
    private int signaturesRequired;     // total signers count (set from signers.size())

    // ===== Gate: Group 2 → Group 3 =====

    private boolean signingNotified;       // notification published to topic
    private java.time.Instant signingNotifiedAt;  // for approval expiry
    private boolean deliveryApproved;      // downstream approved via API

    // ===== Notification dedup — prevents publishing same phase+status repeatedly =====

    private String lastNotifiedPhase;
    private String lastNotifiedStatus;

    // ===== Group 3 results — Packaging & Delivery =====

    private String validationResult; // VALID | NOT_VALID | OUTDATED
    private String envelopeDraftId;
    private boolean additionalDocsUploaded; // tracks whether additional docs were sent to Enigio draft
    private String envelopeTraceId;
    private String envelopeVersionKey;
    private String transferId;

    // ===== Transfer gate — waiting for recipient acceptance =====

    private java.time.Instant transferInitiatedAt;
    private boolean transferAccepted;           // set by TRANSFER webhook
    private boolean transferRejected;           // set by TRANSFER_REJECTED webhook

    // ===== Vendor webhook audit confirmations =====

    private boolean vendorCreateConfirmed;      // CREATE webhook received
    private boolean vendorAmendConfirmed;        // AMENDMENT webhook received
}
