package com.enigio.orchestrator.common.domain;

public enum FlowStep {
    CREATE_DOCUMENT,
    UPLOAD_ATTACHMENT,
    REQUEST_SIGNATURE,
    VERIFY_SIGNATURE,
    FINALIZE_DOCUMENT;

    public FlowStep next() {
        FlowStep[] steps = values();
        int nextOrdinal = this.ordinal() + 1;
        if (nextOrdinal >= steps.length) {
            return null;
        }
        return steps[nextOrdinal];
    }

    public FlowStep previous() {
        int prevOrdinal = this.ordinal() - 1;
        if (prevOrdinal < 0) {
            return null;
        }
        return values()[prevOrdinal];
    }

    public boolean isFirst() {
        return this == CREATE_DOCUMENT;
    }

    public boolean isLast() {
        return this == FINALIZE_DOCUMENT;
    }
}
