package com.orchestrator.starter.audit;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface StepExecutionLogRepository extends MongoRepository<StepExecutionLog, String> {

    List<StepExecutionLog> findByFlowIdOrderByStartedAtAsc(String flowId);
}
