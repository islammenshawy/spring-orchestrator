package com.orchestrator.starter.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;

@Slf4j
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedEventRepository repository;

    public boolean isProcessed(String eventId) {
        return repository.existsById(eventId);
    }

    public boolean markProcessed(String eventId) {
        try {
            repository.save(new ProcessedEvent(eventId));
            return true;
        } catch (DuplicateKeyException e) {
            log.debug("Event {} already processed", eventId);
            return false;
        }
    }
}
