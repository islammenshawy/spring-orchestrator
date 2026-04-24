package com.enigio.orchestrator.common.idempotency;

import com.enigio.orchestrator.common.domain.ProcessedEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProcessedEventRepository extends MongoRepository<ProcessedEvent, String> {
}
