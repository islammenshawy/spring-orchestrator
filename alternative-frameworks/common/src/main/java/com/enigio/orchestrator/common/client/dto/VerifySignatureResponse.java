package com.enigio.orchestrator.common.client.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VerifySignatureResponse {
    private String signatureRequestId;
    private String status;
    private boolean verified;
}
