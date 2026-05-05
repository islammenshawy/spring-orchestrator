package com.dis.instrument.model;

/**
 * All step names in the Enigio instrument flow.
 * Used instead of string literals for type safety and refactoring support.
 *
 * Steps are grouped by business phase:
 *   Phase 1 — Document Preparation: CREATE_DRAFT, REGISTER_DOCUMENT, ADD_ATTACHMENT
 *   Gate 1 — AWAIT_PREPARATION_APPROVAL
 *   Phase 2 — Signing Ceremony: ADD_SIGNERS, SEND_FOR_SIGNING, AWAIT_SIGNATURES
 *   Gate 2 — AWAIT_DELIVERY_APPROVAL
 *   Phase 3 — Packaging & Delivery: VALIDATE_DOCUMENT, CREATE_ENVELOPE, TRANSFER_DOCUMENT
 */
public enum FlowStep {

    // Phase 1
    CREATE_DRAFT,
    REGISTER_DOCUMENT,
    ADD_ATTACHMENT,

    // Gate 1
    AWAIT_PREPARATION_APPROVAL,

    // Phase 2
    ADD_SIGNERS,
    SEND_FOR_SIGNING,
    AWAIT_SIGNATURES,

    // Gate 2
    AWAIT_DELIVERY_APPROVAL,

    // Phase 3
    VALIDATE_DOCUMENT,
    CREATE_ENVELOPE,
    TRANSFER_DOCUMENT;

    /** Match against a currentStep string from MongoDB. */
    public boolean matches(String stepName) {
        return this.name().equals(stepName);
    }

    /** Phase label for a given step (used in approval status responses). */
    public String phase() {
        return switch (this) {
            case CREATE_DRAFT, REGISTER_DOCUMENT, ADD_ATTACHMENT -> "preparation";
            case AWAIT_PREPARATION_APPROVAL -> "awaiting_signing_approval";
            case ADD_SIGNERS, SEND_FOR_SIGNING, AWAIT_SIGNATURES -> "signing";
            case AWAIT_DELIVERY_APPROVAL -> "awaiting_delivery_approval";
            case VALIDATE_DOCUMENT, CREATE_ENVELOPE -> "delivery";
            case TRANSFER_DOCUMENT -> "awaiting_recipient";
        };
    }

    /** True if this step is a gate (parks in MongoDB, exits Kafka). */
    public boolean isGate() {
        return this == AWAIT_PREPARATION_APPROVAL
                || this == AWAIT_SIGNATURES
                || this == AWAIT_DELIVERY_APPROVAL
                || this == TRANSFER_DOCUMENT;
    }
}
