package com.dis.instrument.model;

/**
 * Enigio trace:original document code — determines pricing and capabilities.
 */
public enum DocumentCode {
    /** Negotiable instrument (promissory note, bill of exchange) */
    NEG,
    /** Document of title (bill of lading, warehouse receipt) */
    TTL,
    /** Registry document */
    RGS,
    /** Registry document (no update allowed) */
    RGN,
    /** Agreement / contract */
    AGT
}
