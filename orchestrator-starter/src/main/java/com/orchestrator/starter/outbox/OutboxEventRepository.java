package com.orchestrator.starter.outbox;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

import org.springframework.data.domain.Pageable;

public interface OutboxEventRepository extends MongoRepository<OutboxEvent, String> {

    List<OutboxEvent> findTop100ByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc();

    List<OutboxEvent> findByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc(Pageable pageable);

    long countByFlowIdAndPublishedFalse(String flowId);

    long countByPublishedFalseAndDeadLetteredFalse();

    long countByDeadLetteredTrue();
}
