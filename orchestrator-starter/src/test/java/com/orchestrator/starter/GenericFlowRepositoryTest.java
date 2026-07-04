package com.orchestrator.starter;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.GenericFlowRepository;
import com.orchestrator.starter.domain.OrchestratorFlow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GenericFlowRepositoryTest {

    // ========== Test entity ==========

    static class TestFlow implements OrchestratorFlow {
        private String id;
        private String correlationId;
        private String currentStep;
        private FlowStatus status = FlowStatus.PENDING;
        private int retryCount;
        private int backoffSeconds;
        private Instant nextRetryAt;
        private String errorMessage;
        private Instant updatedAt = Instant.now();

        public TestFlow() {}
        public TestFlow(String id) { this.id = id; }

        public String getId() { return id; }
        public String getCorrelationId() { return correlationId; }
        public void setCorrelationId(String c) { this.correlationId = c; }
        public String getCurrentStep() { return currentStep; }
        public void setCurrentStep(String s) { this.currentStep = s; }
        public FlowStatus getStatus() { return status; }
        public void setStatus(FlowStatus s) { this.status = s; }
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int c) { this.retryCount = c; }
        public int getBackoffSeconds() { return backoffSeconds; }
        public void setBackoffSeconds(int s) { this.backoffSeconds = s; }
        public Instant getNextRetryAt() { return nextRetryAt; }
        public void setNextRetryAt(Instant i) { this.nextRetryAt = i; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String m) { this.errorMessage = m; }
        public Instant getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(Instant i) { this.updatedAt = i; }
    }

    private MongoTemplate mongoTemplate;
    private GenericFlowRepository<TestFlow> repo;

    @BeforeEach
    void setUp() {
        mongoTemplate = mock(MongoTemplate.class);
        repo = new GenericFlowRepository<>(mongoTemplate, TestFlow.class);
    }

    // ========== save ==========

    @Test
    void save_delegatesToMongoTemplate() {
        var flow = new TestFlow("f-1");
        when(mongoTemplate.save(flow)).thenReturn(flow);

        var result = repo.save(flow);

        assertSame(flow, result);
        verify(mongoTemplate).save(flow);
    }

    // ========== findById ==========

    @Test
    void findById_found_returnsOptionalWithValue() {
        var flow = new TestFlow("f-1");
        when(mongoTemplate.findById("f-1", TestFlow.class)).thenReturn(flow);

        var result = repo.findById("f-1");

        assertTrue(result.isPresent());
        assertSame(flow, result.get());
    }

    @Test
    void findById_notFound_returnsEmpty() {
        when(mongoTemplate.findById("missing", TestFlow.class)).thenReturn(null);

        var result = repo.findById("missing");

        assertTrue(result.isEmpty());
    }

    // ========== existsById ==========

    @Test
    void existsById_true_whenFlowExists() {
        // existsById now uses mongoTemplate.exists() (avoids fetching the full document)
        when(mongoTemplate.exists(any(Query.class), eq(TestFlow.class))).thenReturn(true);

        assertTrue(repo.existsById("f-1"));
    }

    @Test
    void existsById_false_whenFlowMissing() {
        when(mongoTemplate.exists(any(Query.class), eq(TestFlow.class))).thenReturn(false);

        assertFalse(repo.existsById("missing"));
    }

    // ========== findByCorrelationId ==========

    @Test
    void findByCorrelationId_found() {
        var flow = new TestFlow("f-1");
        flow.setCorrelationId("corr-123");
        when(mongoTemplate.findOne(any(Query.class), eq(TestFlow.class))).thenReturn(flow);

        var result = repo.findByCorrelationId("corr-123");

        assertTrue(result.isPresent());
        assertEquals("corr-123", result.get().getCorrelationId());
    }

    @Test
    void findByCorrelationId_notFound() {
        when(mongoTemplate.findOne(any(Query.class), eq(TestFlow.class))).thenReturn(null);

        var result = repo.findByCorrelationId("no-such-corr");

        assertTrue(result.isEmpty());
    }

    // ========== findByStatusAndUpdatedAtBefore ==========

    @Test
    void findByStatusAndUpdatedAtBefore_returnsMatchingFlows() {
        var flow1 = new TestFlow("f-1");
        var flow2 = new TestFlow("f-2");
        when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                .thenReturn(List.of(flow1, flow2));

        var result = repo.findByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, Instant.now());

        assertEquals(2, result.size());
    }

    // ========== countByStatusAndUpdatedAtBefore ==========

    @Test
    void countByStatusAndUpdatedAtBefore_delegatesToTemplate() {
        when(mongoTemplate.count(any(Query.class), eq(TestFlow.class))).thenReturn(5L);

        long count = repo.countByStatusAndUpdatedAtBefore(FlowStatus.IN_PROGRESS, Instant.now());

        assertEquals(5L, count);
    }

    // ========== findByStatus ==========

    @Test
    void findByStatus_returnsMatchingFlows() {
        var flow = new TestFlow("f-1");
        when(mongoTemplate.find(any(Query.class), eq(TestFlow.class)))
                .thenReturn(List.of(flow));

        var result = repo.findByStatus(FlowStatus.COMPLETED);

        assertEquals(1, result.size());
    }

    // ========== findAll ==========

    @Test
    void findAll_returnsAllFlows() {
        var flow1 = new TestFlow("f-1");
        var flow2 = new TestFlow("f-2");
        when(mongoTemplate.findAll(TestFlow.class)).thenReturn(List.of(flow1, flow2));

        var result = repo.findAll();

        assertEquals(2, result.size());
    }

    // ========== count ==========

    @Test
    void count_delegatesToTemplate() {
        when(mongoTemplate.count(any(Query.class), eq(TestFlow.class))).thenReturn(42L);

        assertEquals(42L, repo.count());
    }

    // ========== deleteById ==========

    @Test
    void deleteById_delegatesToTemplate() {
        repo.deleteById("f-1");

        verify(mongoTemplate).remove(any(Query.class), eq(TestFlow.class));
    }

    // ========== delete ==========

    @Test
    void delete_delegatesToTemplate() {
        var flow = new TestFlow("f-1");
        repo.delete(flow);

        verify(mongoTemplate).remove(flow);
    }

    // ========== Unsupported operations ==========

    @Test
    void saveAll_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.saveAll(List.of()));
    }

    @Test
    void findAllById_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.findAllById(List.of("a")));
    }

    @Test
    void deleteAllById_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.deleteAllById(List.of("a")));
    }

    @Test
    void deleteAllEntities_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.deleteAll(List.of()));
    }

    @Test
    void deleteAll_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.deleteAll());
    }

    @Test
    void insert_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.insert(new TestFlow()));
    }

    @Test
    void insertAll_throwsUnsupported() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.insert(List.of()));
    }
}
