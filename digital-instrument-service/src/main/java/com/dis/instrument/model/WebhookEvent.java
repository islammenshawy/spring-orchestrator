package com.dis.instrument.model;

/**
 * Enigio webhook event types (v3.3).
 * Received at POST /webhooks/enigio from the vendor.
 */
public enum WebhookEvent {
    PARTIALLY_SIGNED,
    FULLY_SIGNED,
    SIGNATURE_REJECTED,
    TRANSFER,
    TRANSFER_REJECTED,
    TRANSFER_CANCELLED,
    CREATE,
    AMENDMENT,
    INVALIDATE
}
