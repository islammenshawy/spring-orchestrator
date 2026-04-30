package com.enigio.orchestrator.mock.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Matches Enigio trace:original API v3.3 — RequiredSignatureDTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequiredSignatureDTO {
    private Long id;
    private String traceOriginalId;
    private String capacityOfSignatory;
    private Integer documentVersion;
    private String organisation;
    private String role;
    private String status; // PENDING | SIGNED | REJECTED | NO_SIGNATURE
    private String linkCreatedAt;
    private String linkExpiresAt;
    private List<SignerDTO> signers;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SignerDTO {
        private String email;
        private String name;
        private String phone;
        private String linkId;
        private String signingLink;
    }
}
