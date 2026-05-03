package com.orchestrator.starter.outbox;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OutboxEventRepository extends MongoRepository<OutboxEvent, String> {

    List<OutboxEvent> findTop100ByPublishedFalseOrderByCreatedAtAsc();

    long countByFlowIdAndPublishedFalse(String flowId);
}
