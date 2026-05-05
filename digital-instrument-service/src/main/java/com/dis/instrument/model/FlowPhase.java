package com.dis.instrument.model;

/**
 * Notification phase values published to the downstream topic.
 * Downstream systems use these to track flow progress.
 */
public enum FlowPhase {
    PREPARATION_COMPLETE,
    APPROVAL_EXPIRED,
    SIGNATURE_RECEIVED,
    ALL_SIGNATURES_COMPLETE,
    SIGNING_EXPIRED,
    SIGNATURE_REJECTED,
    SIGNING_COMPLETE,
    TRANSFER_INITIATED,
    TRANSFER_REJECTED,
    TRANSFER_EXPIRED,
    FLOW_COMPLETE,
    FLOW_CANCELLED
}
