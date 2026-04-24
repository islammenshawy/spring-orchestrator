package com.enigio.orchestrator.saga.saga;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SagaStepLogRepository extends MongoRepository<SagaStepLog, String> {

    List<SagaStepLog> findByFlowIdOrderByStartedAtAsc(String flowId);
}
