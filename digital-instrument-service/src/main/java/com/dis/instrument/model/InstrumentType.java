package com.dis.instrument.model;

/**
 * Types of trade instruments this service can orchestrate.
 * Maps to Enigio documentType recommended values.
 */
public enum InstrumentType {
    PROMISSORY_NOTE("Promissory note"),
    BILL_OF_EXCHANGE("Bill of exchange"),
    BILL_OF_LADING("Bill of lading"),
    LETTER_OF_CREDIT("Letter of credit"),
    GUARANTEE("Guarantee"),
    CERTIFICATE("Certificate"),
    INVOICE("Invoice"),
    CONTRACT("Contract"),
    AGREEMENT("Agreement"),
    GENERIC("Generic");

    private final String enigioValue;

    InstrumentType(String enigioValue) {
        this.enigioValue = enigioValue;
    }

    /** Value sent to Enigio API documentType field */
    public String toEnigioValue() {
        return enigioValue;
    }
}
