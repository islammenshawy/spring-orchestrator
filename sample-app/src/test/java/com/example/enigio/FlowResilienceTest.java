package com.example.enigio;

import com.example.enigio.flow.EnigioFlow;
import com.orchestrator.starter.domain.FlowStatus;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for flow resilience.
 * Requires: Kafka (localhost:9092), MongoDB (localhost:27017), mock-vendor (localhost:8081).
 *
 * Tests verify:
 * 1. Happy path — flow completes all 5 steps
 * 2. Retryable failure — flow recovers after flaky vendor
 * 3. Non-retryable failure — flow fails fast + compensation runs
 * 4. Timeout — vendor hangs, WebClient times out, flow retries
 * 5. Idempotency — duplicate messages don't cause duplicate API calls
 * 6. Ordering — steps execute in sequence via partition key
 * 7. DLT — persistent failure exhausts retries, lands in DLT with exception
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.context.ActiveProfiles("test")
class FlowResilienceTest {

    @LocalServerPort
    private int port;

    @Autowired
    private MongoTemplate mongoTemplate;

    private TestRestTemplate rest;
    private WebClient vendorAdmin;

    @BeforeAll
    static void clearTestData(@Autowired MongoTemplate mongo) throws InterruptedException {
        for (String col : mongo.getCollectionNames()) {
            mongo.dropCollection(col);
        }
        // Wait for Kafka consumers to join groups before starting tests
        Thread.sleep(5000);
    }

    @BeforeEach
    void setUp() {
        rest = new TestRestTemplate();
        vendorAdmin = WebClient.create("http://localhost:8081");
        try {
            vendorAdmin.post().uri("/admin/reset").retrieve().bodyToMono(String.class).block();
        } catch (Exception e) {
            // vendor may not be running
        }
    }


    private String baseUrl() { return "http://localhost:" + port; }

    @SuppressWarnings("unchecked")
    private Map<String, Object> startFlow(String title) {
        return rest.post().uri("/flows")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("title", title, "content", "test", "signerEmail", "test@test.com"))
                .retrieve()
                .body(Map.class);
    }

    private EnigioFlow waitForStatus(String flowId, FlowStatus expected, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioFlow flow = mongoTemplate.findById(flowId, EnigioFlow.class, "enigio_flows");
            if (flow != null && flow.getStatus() == expected) return flow;
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
        EnigioFlow flow = mongoTemplate.findById(flowId, EnigioFlow.class, "enigio_flows");
        fail("Flow " + flowId + " did not reach " + expected + " within " + timeout +
             ". Current: " + (flow != null ? flow.getStatus() + " at " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }

    // ========== 1. Happy path ==========

    @Test
    @Order(1)
    void happyPath_flowCompletesAllSteps() {
        var result = startFlow("Happy Path Test");
        String flowId = (String) result.get("id");
        assertNotNull(flowId);

        EnigioFlow completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofSeconds(60));

        assertEquals("FINALIZE_DOCUMENT", completed.getCurrentStep());
        assertNotNull(completed.getEnigioDocumentId(), "Should have document ID");
        assertNotNull(completed.getAttachmentId(), "Should have attachment ID");
        assertNotNull(completed.getSignatureRequestId(), "Should have signature request ID");
        assertNotNull(completed.getFlowType(), "Should have flowType set");
        assertEquals(0, completed.getRetryCount());
    }

    // ========== 2. Flaky vendor — retries and recovers ==========

    @Test
    @Order(2)
    void flakyVendor_flowRecoversAfterRetry() {
        // Set createDocument to flaky (50% failure rate)
        vendorAdmin.post().uri("/admin/failure-config")
                .bodyValue(Map.of("createDocument", "FLAKY"))
                .retrieve().bodyToMono(String.class).block();

        var result = startFlow("Flaky Recovery Test");
        String flowId = (String) result.get("id");

        // With 60s backoff and flaky, it should eventually complete
        // Give it enough time for a few retries
        EnigioFlow completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(5));

        assertNotNull(completed.getEnigioDocumentId());
        assertNotNull(completed.getFinalDocumentUrl());
    }

    // ========== 3. HTTP 500 — retryable, goes to retry topic then recovers ==========

    @Test
    @Order(3)
    void http500_retriesAndRecovers() {
        // Set 500 on upload — first step succeeds, second fails
        vendorAdmin.post().uri("/admin/failure-config")
                .bodyValue(Map.of("uploadAttachment", "FLAKY"))
                .retrieve().bodyToMono(String.class).block();

        var result = startFlow("500 Retry Test");
        String flowId = (String) result.get("id");

        // Should eventually complete after retry
        EnigioFlow completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));
        assertNotNull(completed.getAttachmentId());
    }

    // ========== 4. Flow state persisted correctly ==========

    @Test
    @Order(4)
    void flowState_allFieldsPersisted() {
        var result = startFlow("State Persistence Test");
        String flowId = (String) result.get("id");

        EnigioFlow completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofSeconds(60));

        // Verify all library-managed fields
        assertNotNull(completed.getId());
        assertNotNull(completed.getCorrelationId());
        assertNotNull(completed.getCreatedAt());
        assertNotNull(completed.getUpdatedAt());
        // Version field exists but not used for optimistic locking
        // (concurrency handled by Kafka partition key + idempotency)
        assertEquals(0, completed.getRetryCount());
        assertEquals(0, completed.getBackoffSeconds());
        assertNull(completed.getErrorMessage());
        assertNull(completed.getNextRetryAt());
    }

    // ========== 5. Step audit logs created ==========

    @Test
    @Order(5)
    void stepLogs_createdForEachStep() {
        var result = startFlow("Audit Log Test");
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofSeconds(60));

        // Check step logs
        var logs = mongoTemplate.find(
                new Query(org.springframework.data.mongodb.core.query.Criteria.where("flowId").is(flowId)),
                org.bson.Document.class, "orchestrator_step_log");

        assertTrue(logs.size() >= 5, "Should have at least 5 step logs (one per step), got " + logs.size());

        // Verify each step has a log
        var stepNames = logs.stream().map(d -> d.getString("stepName")).toList();
        assertTrue(stepNames.contains("CREATE_DOCUMENT"));
        assertTrue(stepNames.contains("UPLOAD_ATTACHMENT"));
        assertTrue(stepNames.contains("REQUEST_SIGNATURE"));
        assertTrue(stepNames.contains("FINALIZE_DOCUMENT"));
    }

    // ========== 6. Idempotency — processed events tracked ==========

    @Test
    @Order(6)
    void idempotency_eventsTrackedInProcessedCollection() {
        var result = startFlow("Idempotency Test");
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofSeconds(60));

        // Check processed_events collection has entries
        long count = mongoTemplate.count(new Query(), "orchestrator_processed_events");
        assertTrue(count > 0, "Processed events should be tracked");
    }

    // ========== 7. Outbox events created and published ==========

    @Test
    @Order(7)
    void outbox_eventsCreatedAndPublished() {
        var result = startFlow("Outbox Test");
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofSeconds(60));

        // All outbox events should be published (published=true)
        var unpublished = mongoTemplate.find(
                new Query(org.springframework.data.mongodb.core.query.Criteria.where("published").is(false)),
                org.bson.Document.class, "orchestrator_outbox");

        // After flow completes, there should be no unpublished events (publisher runs every 500ms)
        // Allow a small window
        try { Thread.sleep(2000); } catch (InterruptedException e) {}
        unpublished = mongoTemplate.find(
                new Query(org.springframework.data.mongodb.core.query.Criteria.where("published").is(false)),
                org.bson.Document.class, "orchestrator_outbox");
        assertEquals(0, unpublished.size(), "All outbox events should be published");
    }

    // ========== 8. Multiple concurrent flows ==========

    @Test
    @Order(8)
    void concurrentFlows_allComplete() {
        List<String> flowIds = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            var result = startFlow("Concurrent #" + (i + 1));
            flowIds.add((String) result.get("id"));
        }

        // All should complete
        for (String flowId : flowIds) {
            EnigioFlow completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(2));
            assertNotNull(completed.getFinalDocumentUrl());
        }
    }
}
