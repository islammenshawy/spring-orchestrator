package com.enigio.orchestrator.mock.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MockDocument {
    private String documentId;
    private String title;
    private String content;
    private String metadata;
    private String attachmentId;
    private String signatureRequestId;
    private int verifyCallCount;
    private boolean finalized;
}
