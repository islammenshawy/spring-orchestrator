package com.orchestrator.starter.domain;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Base repository interface for flow entities.
 * Users extend this with their concrete flow type.
 *
 * Usage:
 * <pre>
 * public interface MyFlowRepository extends OrchestratorFlowRepository&lt;MyFlow&gt; {
 *     // add custom queries if needed
 * }
 * </pre>
 */
@NoRepositoryBean
public interface OrchestratorFlowRepository<F extends OrchestratorFlow> extends MongoRepository<F, String> {

    Optional<F> findByCorrelationId(String correlationId);

    List<F> findByStatusAndUpdatedAtBefore(FlowStatus status, Instant threshold);

    List<F> findByStatus(FlowStatus status);

    long countByStatusAndUpdatedAtBefore(FlowStatus status, Instant threshold);
}
