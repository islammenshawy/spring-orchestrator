package com.enigio.orchestrator.saga.outbox;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OutboxEventRepository extends MongoRepository<OutboxEvent, String> {

    List<OutboxEvent> findTop50ByPublishedFalseOrderByCreatedAtAsc();
}
