package com.dis.instrument.core.model;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface AdditionalDocumentRepository extends MongoRepository<AdditionalDocument, String> {

    List<AdditionalDocument> findByInstrumentId(String instrumentId);
}
