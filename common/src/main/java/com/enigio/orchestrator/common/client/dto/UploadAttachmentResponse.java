package com.enigio.orchestrator.common.client.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UploadAttachmentResponse {
    private String attachmentId;
    private String status;
}
