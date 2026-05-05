package com.dis.instrument.model;

/**
 * Signing status values tracked on the flow entity.
 * Set by webhook events or polling fallback.
 */
public enum SigningStatus {
    PENDING,
    PARTIALLY_SIGNED,
    SIGNED,
    REJECTED,
    EXPIRED
}
