package com.dis.instrument.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Binary document stored in MongoDB, referenced by flow via ID only.
 * Keeps large payloads out of Kafka messages — the flow entity only stores document IDs.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "dis_additional_documents")
public class AdditionalDocument {

    @Id
    private String id;

    /** Instrument ID — null if uploaded before flow starts, linked later. */
    @Indexed
    private String instrumentId;

    private String filename;
    private String contentType;

    /** Base-64 encoded binary content. */
    private String data;

    private String sha256Hash;
    private long sizeBytes;
    private Instant uploadedAt;
}
