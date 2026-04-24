package com.enigio.orchestrator.common.idempotency;

import com.enigio.orchestrator.common.domain.ProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedEventRepository repository;

    public boolean isProcessed(String eventId) {
        return repository.existsById(eventId);
    }

    public boolean tryMarkAsProcessed(String eventId) {
        try {
            repository.save(new ProcessedEvent(eventId));
            return true;
        } catch (DuplicateKeyException e) {
            log.warn("Event {} already processed, skipping", eventId);
            return false;
        }
    }
}
