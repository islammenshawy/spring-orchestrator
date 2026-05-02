package com.dis.instrument.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Published to the notification topic at each group boundary.
 * Downstream systems consume these to track progress and take action.
 *
 * Every notification includes actionable URLs so downstream never needs
 * to construct paths — just follow the links in the payload.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlowNotification {

    // ===== Identity — downstream uses these to correlate =====

    /** The DIS instrument ID — correlate with your internal instrument/flow record. */
    private String instrumentId;
    private String correlationId;
    private String reference;
    private String instrumentType;

    // ===== Phase + status =====

    private String phase;           // PREPARATION_COMPLETE | SIGNING_COMPLETE | SIGNATURE_RECEIVED |
                                    // ALL_SIGNATURES_COMPLETE | SIGNATURE_REJECTED | SIGNING_EXPIRED |
                                    // APPROVAL_EXPIRED | FLOW_COMPLETE | FLOW_CANCELLED
    private String status;          // AWAITING_APPROVAL | COMPLETED | SIGNED | REJECTED | CANCELLED
    private String currentStep;     // step name at time of notification

    // ===== Vendor state (populated when available) =====

    private String traceOriginalId;
    private String signingStatus;
    private String transferId;

    // ===== Actionable URLs — downstream follows these, no path construction needed =====

    private String approveUrl;              // POST — approve next phase
    private String cancelUrl;               // POST — cancel the flow
    private String statusUrl;               // GET — poll flow status
    private String approvalStatusUrl;       // GET — check approval flags
    private String vendorSyncUrl;           // GET — reconcile with Enigio ledger
    private String additionalDocumentsUrl;  // POST — upload additional docs for envelope

    private Instant timestamp;
}
