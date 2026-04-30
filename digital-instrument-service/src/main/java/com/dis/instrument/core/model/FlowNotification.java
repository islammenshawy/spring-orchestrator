package com.dis.instrument.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Published to the notification topic at each group boundary.
 * Downstream systems consume these to track progress and approve next group.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlowNotification {

    private String flowId;
    private String correlationId;
    private String reference;
    private String instrumentType;
    private String phase;           // PREPARATION_COMPLETE | SIGNING_COMPLETE | FLOW_COMPLETE
    private String status;          // AWAITING_APPROVAL | COMPLETED
    private String traceOriginalId;
    private String signingStatus;
    private String transferId;
    private String approveUrl;      // POST /flows/enigio-instrument/{id}/approve
    private Instant timestamp;
}
