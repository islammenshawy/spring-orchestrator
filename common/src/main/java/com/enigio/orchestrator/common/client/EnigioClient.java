package com.enigio.orchestrator.common.client;

import com.enigio.orchestrator.common.client.dto.*;

public interface EnigioClient {

    CreateDocumentResponse createDocument(CreateDocumentRequest request);

    UploadAttachmentResponse uploadAttachment(String documentId, UploadAttachmentRequest request);

    RequestSignatureResponse requestSignature(String documentId, RequestSignatureRequest request);

    VerifySignatureResponse verifySignature(String documentId, String signatureRequestId);

    FinalizeDocumentResponse finalizeDocument(String documentId);
}
