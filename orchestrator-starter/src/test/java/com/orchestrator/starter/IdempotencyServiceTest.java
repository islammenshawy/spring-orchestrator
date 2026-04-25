package com.orchestrator.starter;

import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.idempotency.ProcessedEvent;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IdempotencyServiceTest {

    @Test
    void markProcessedReturnsTrueOnFirstCall() {
        var repo = mock(ProcessedEventRepository.class);
        when(repo.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var service = new IdempotencyService(repo);
        assertTrue(service.markProcessed("event-1"));
        verify(repo).save(any(ProcessedEvent.class));
    }

    @Test
    void markProcessedReturnsFalseOnDuplicate() {
        var repo = mock(ProcessedEventRepository.class);
        when(repo.save(any())).thenThrow(new DuplicateKeyException("dup"));

        var service = new IdempotencyService(repo);
        assertFalse(service.markProcessed("event-1"));
    }

    @Test
    void isProcessedDelegatesToRepository() {
        var repo = mock(ProcessedEventRepository.class);
        when(repo.existsById("event-1")).thenReturn(true);
        when(repo.existsById("event-2")).thenReturn(false);

        var service = new IdempotencyService(repo);
        assertTrue(service.isProcessed("event-1"));
        assertFalse(service.isProcessed("event-2"));
    }
}
