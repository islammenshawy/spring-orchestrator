package com.enigio.orchestrator.common.client.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UploadAttachmentRequest {
    private String fileName;
    private String fileContent;
}
