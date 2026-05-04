package com.enigio.orchestrator.sm.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SmStepLogRepository extends MongoRepository<SmStepLog, String> {

    List<SmStepLog> findByFlowIdOrderByCreatedAtAsc(String flowId);
}
