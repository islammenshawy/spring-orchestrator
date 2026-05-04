package com.enigio.orchestrator.common.domain;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface DocumentFlowRepository extends MongoRepository<DocumentFlow, String> {

    Optional<DocumentFlow> findByCorrelationId(String correlationId);

    List<DocumentFlow> findByStatusAndUpdatedAtBefore(FlowStatus status, Instant threshold);

    List<DocumentFlow> findByStatus(FlowStatus status);
}
