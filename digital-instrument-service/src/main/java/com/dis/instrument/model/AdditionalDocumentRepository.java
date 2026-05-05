package com.dis.instrument.model;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface AdditionalDocumentRepository extends MongoRepository<AdditionalDocument, String> {

    List<AdditionalDocument> findByInstrumentId(String instrumentId);
}
