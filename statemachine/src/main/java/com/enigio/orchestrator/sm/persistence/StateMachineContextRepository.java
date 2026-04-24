package com.enigio.orchestrator.sm.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.List;

public interface StateMachineContextRepository extends MongoRepository<StateMachineContextDocument, String> {

    List<StateMachineContextDocument> findByStateNotInAndUpdatedAtBefore(List<String> terminalStates, Instant threshold);
}
