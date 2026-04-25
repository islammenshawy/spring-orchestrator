package com.orchestrator.starter.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;

@Slf4j
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedEventRepository repository;

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
