package com.orchestrator.starter.domain;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface StepCompletionRepository extends MongoRepository<StepCompletion, String> {
}
