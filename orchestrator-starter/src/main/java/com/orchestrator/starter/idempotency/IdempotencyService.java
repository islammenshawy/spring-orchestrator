package com.orchestrator.starter.idempotency;

import com.orchestrator.starter.autoconfigure.OrchestratorMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;

@Slf4j
public class IdempotencyService {

    private final ProcessedEventRepository repository;
    private final OrchestratorMetrics metrics;

    public IdempotencyService(ProcessedEventRepository repository) {
        this(repository, null);
    }

    public IdempotencyService(ProcessedEventRepository repository, OrchestratorMetrics metrics) {
        this.repository = repository;
        this.metrics = metrics;
    }

    /**
     * Single-query idempotency: try to insert. If duplicate → already processed.
     * Returns true if this is the first time (proceed with execution).
     * Returns false if already processed (skip).
     *
     * One query instead of two (was: existsById + save = 2 queries).
     */
    public boolean tryProcess(String eventId) {
        try {
            repository.save(new ProcessedEvent(eventId));
            return true; // first time — proceed
        } catch (DuplicateKeyException e) {
            if (metrics != null) metrics.idempotencyDuplicate();
            log.debug("Event {} already processed, skipping", eventId);
            return false; // duplicate — skip
        }
    }

    /**
     * Check-only (no write). Used for fast-path skip in consumer.
     */
    public boolean isProcessed(String eventId) {
        return repository.existsById(eventId);
    }
}
