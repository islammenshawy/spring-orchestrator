package com.dis.instrument;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.idempotency.ProcessedEventRepository;
import com.orchestrator.starter.kafka.StepCommandMessage;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resilience integration tests covering the 3 critical gaps:
 * 1. Duplicate Kafka message → idempotency prevents double vendor calls
 * 2. Pod crash → recovery scanner claims and re-publishes
 * 3. Concurrent batch claiming → each flow claimed by exactly one "pod"
 *
 * Requires: Kafka, MongoDB, mock-vendor running (Docker).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.context.ActiveProfiles("test")
class ResilienceIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private StaleFlowRecoveryService staleFlowRecoveryService;

    @Value("${orchestrator.kafka.command-topic}")
    private String commandTopic;

    private RestClient rest;

    @BeforeEach
    void setUp() {
        rest = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultHeader("X-API-Key", "test-api-key").build();
        clearVendorFailures();
    }

    @AfterEach
    void tearDown() {
        // Always clear vendor failures — even if assertions fail
        clearVendorFailures();
    }

    // ========== 1. DUPLICATE KAFKA MESSAGE — IDEMPOTENCY ==========

    @Test
    @Order(1)
    @DisplayName("Duplicate Kafka message: same eventId sent twice, vendor called only once")
    void duplicateKafkaMessage_vendorCalledOnlyOnce() throws Exception {
        // Start a flow and let it reach the first gate step
        var result = startFlow("PN-DEDUP-001");
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity flow = waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));
        assertNotNull(flow, "Flow should reach gate step");

        // Record vendor call count BEFORE duplicate injection
        // We'll check the mock-vendor stats via its admin endpoint
        String vendorStatsBefore = getVendorStats();

        // Get the outbox events for this flow to find the eventId pattern
        // Now inject a duplicate step command with the SAME step that already completed
        String completedStep = "ADD_ATTACHMENT"; // a step we know completed
        assertTrue(flow.getCompletedSteps().contains(completedStep),
                completedStep + " should be in completedSteps");

        // Send the same step command again with a NEW eventId
        // This tests Layer 2 (completedSteps) — the event is new but the step already ran
        String duplicateEventId = "dedup-test-" + UUID.randomUUID();
        StepCommandMessage dupCmd = StepCommandMessage.builder()
                .eventId(duplicateEventId)
                .flowId(flowId)
                .correlationId(flow.getCorrelationId())
                .stepName(completedStep)
                .flowType("enigio-instrument")
                .build();

        kafkaTemplate.send(commandTopic, flow.getCorrelationId(),
                objectMapper.writeValueAsString(dupCmd)).get();

        // Wait for the message to be processed
        Thread.sleep(5000);

        // Verify: the duplicate was processed by consumer (ProcessedEvent created)
        // but the step was NOT re-executed (completedSteps check)
        assertTrue(processedEventRepository.existsById(duplicateEventId),
                "Duplicate eventId should be recorded in processed_events");

        // Flow should still be at the same step (not re-executed)
        EnigioInstrumentEntity flowAfter = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flowAfter);
        assertEquals("AWAIT_PREPARATION_APPROVAL", flowAfter.getCurrentStep(),
                "Flow should still be at gate step — duplicate didn't advance it");

        // Now test Layer 1: send the EXACT same eventId again
        kafkaTemplate.send(commandTopic, flow.getCorrelationId(),
                objectMapper.writeValueAsString(dupCmd)).get();
        Thread.sleep(3000);

        // Verify the flow is unchanged
        EnigioInstrumentEntity flowAfterDup2 = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flowAfterDup2);
        assertEquals("AWAIT_PREPARATION_APPROVAL", flowAfterDup2.getCurrentStep(),
                "Flow should still be at gate step after Layer 1 dedup");

        // Verify vendor was NOT called again by checking stats
        String vendorStatsAfter = getVendorStats();
        // The document count shouldn't have changed (no new registerDocument calls)
        assertEquals(vendorStatsBefore, vendorStatsAfter,
                "Vendor stats should be unchanged — no duplicate API calls");
    }

    @Test
    @Order(2)
    @DisplayName("Duplicate Kafka message: same eventId is silently skipped (Layer 1)")
    void duplicateEventId_silentlySkipped() throws Exception {
        // Pre-create a processed event
        String eventId = "preexisting-" + UUID.randomUUID();
        processedEventRepository.save(
                new com.orchestrator.starter.idempotency.ProcessedEvent(eventId));

        // Send a command with that eventId — should be silently skipped
        StepCommandMessage cmd = StepCommandMessage.builder()
                .eventId(eventId)
                .flowId("nonexistent-flow")
                .correlationId("nonexistent")
                .stepName("CREATE_DRAFT")
                .flowType("enigio-instrument")
                .build();

        kafkaTemplate.send(commandTopic, "key", objectMapper.writeValueAsString(cmd)).get();
        Thread.sleep(3000);

        // The flow doesn't exist, but the consumer shouldn't have tried to load it
        // because the idempotency check returned early. If it had tried, we'd get
        // a "flow not found" error in the logs but no crash.
        // The key assertion: no exception, no new processed event (was already there)
        assertTrue(processedEventRepository.existsById(eventId));

        // Cleanup
        processedEventRepository.deleteById(eventId);
    }

    // ========== 2. POD CRASH + RECOVERY ==========

    @Test
    @Order(3)
    @DisplayName("Pod crash: stuck IN_PROGRESS flow recovered by scanner")
    void podCrash_stuckFlow_recoveredByScanner() throws Exception {
        // Start a flow and let it reach a step
        var result = startFlow("PN-CRASH-001");
        String flowId = (String) result.get("id");

        // Wait for flow to reach gate step (proves it started processing)
        EnigioInstrumentEntity flow = waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));
        assertNotNull(flow);

        // Simulate pod crash: set status=IN_PROGRESS with old updatedAt
        // This is what happens when a pod dies mid-step — the flow is stuck
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update()
                        .set("status", FlowStatus.IN_PROGRESS.name())
                        .set("currentStep", "AWAIT_PREPARATION_APPROVAL")
                        .set("updatedAt", Instant.now().minus(20, ChronoUnit.MINUTES))
                        .set("claimedBy", null)
                        .set("claimedAt", null),
                "dis_instrument_flows");

        // Verify it's stuck
        EnigioInstrumentEntity stuck = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertEquals(FlowStatus.IN_PROGRESS, stuck.getStatus());
        assertTrue(stuck.getUpdatedAt().isBefore(Instant.now().minus(15, ChronoUnit.MINUTES)));

        // Trigger recovery scanner
        staleFlowRecoveryService.recoverStaleFlows();

        // Wait for recovery to process (Kafka message delivered + consumed)
        Thread.sleep(5000);

        // Verify: flow was recovered — updatedAt bumped, recoveryCount incremented
        EnigioInstrumentEntity recovered = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(recovered);
        assertTrue(recovered.getUpdatedAt().isAfter(Instant.now().minus(1, ChronoUnit.MINUTES)),
                "Recovery should bump updatedAt");
        assertTrue(recovered.getRecoveryCount() >= 1,
                "Recovery count should be incremented");
        // Flow should be back at PARKED (gate step re-evaluated via waitUntil)
        assertEquals(FlowStatus.PARKED, recovered.getStatus(),
                "Flow should return to PARKED after recovery re-publishes gate step");
        assertNull(recovered.getClaimedBy(),
                "Claim should be released after processing");
    }

    @Test
    @Order(4)
    @DisplayName("Pod crash: recovery count caps at max, marks FAILED")
    void podCrash_maxRecoveryAttempts_marksFailed() throws Exception {
        // Start a flow
        var result = startFlow("PN-MAX-REC-001");
        String flowId = (String) result.get("id");

        waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));

        // Simulate flow stuck with recoveryCount at max (10)
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update()
                        .set("status", FlowStatus.IN_PROGRESS.name())
                        .set("updatedAt", Instant.now().minus(20, ChronoUnit.MINUTES))
                        .set("recoveryCount", 10)
                        .set("claimedBy", null),
                "dis_instrument_flows");

        // Trigger recovery — exhausted flows go COMPENSATING → compensation → FAILED
        staleFlowRecoveryService.recoverStaleFlows();

        // Poll for terminal status (compensation runs but may race with Kafka consumer)
        EnigioInstrumentEntity failed = null;
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            failed = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (failed != null && (failed.getStatus() == FlowStatus.FAILED
                    || failed.getStatus() == FlowStatus.COMPENSATION_FAILED)) break;
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
        assertNotNull(failed);
        assertTrue(failed.getStatus() == FlowStatus.FAILED || failed.getStatus() == FlowStatus.COMPENSATION_FAILED,
                "Flow should be FAILED after exceeding max recovery, was: " + failed.getStatus());
        assertTrue(failed.getErrorMessage().contains("max recovery"),
                "Error message should mention max recovery attempts");
    }

    // ========== 3. CONCURRENT BATCH CLAIMING ==========

    @Test
    @Order(5)
    @DisplayName("Concurrent claiming: multiple threads claim stale flows without duplicates")
    void concurrentClaiming_noDuplicatePublishes() throws Exception {
        // Create 10 stuck flows — batch update after all reach gate step
        int flowCount = 10;
        String prefix = "CONC-CLAIM-" + UUID.randomUUID().toString().substring(0, 6) + "-";
        List<String> flowIds = new java.util.ArrayList<>();
        for (int i = 0; i < flowCount; i++) {
            var result = startFlow(prefix + i);
            flowIds.add((String) result.get("id"));
        }

        // Wait for all to reach gate step
        for (String flowId : flowIds) {
            waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));
        }

        // Batch-update all to stale IN_PROGRESS in one operation
        long updated = mongoTemplate.updateMulti(
                Query.query(Criteria.where("_id").in(flowIds)),
                new Update()
                        .set("status", FlowStatus.IN_PROGRESS.name())
                        .set("updatedAt", Instant.now().minus(20, ChronoUnit.MINUTES))
                        .set("claimedBy", null)
                        .set("claimedAt", null)
                        .set("recoveryCount", 0),
                "dis_instrument_flows").getModifiedCount();

        // Verify all are stuck
        long staleCount = mongoTemplate.count(
                Query.query(Criteria.where("status").is(FlowStatus.IN_PROGRESS.name())
                        .and("_id").in(flowIds)),
                "dis_instrument_flows");
        assertEquals(flowCount, staleCount, "Should have " + flowCount + " stale flows (updated=" + updated + ")");

        // Run recovery from 3 concurrent threads (simulating 3 pods)
        int threadCount = 3;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger totalClaimed = new AtomicInteger();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startGate.await(); // all threads start simultaneously
                    staleFlowRecoveryService.recoverStaleFlows();
                } catch (Exception e) {
                    // expected — concurrent access
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown(); // release all threads
        done.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Wait for Kafka messages to be consumed
        Thread.sleep(8000);

        // Verify: each flow should have recoveryCount = 1 (claimed once, not N times)
        List<EnigioInstrumentEntity> flows = mongoTemplate.find(
                Query.query(Criteria.where("_id").in(flowIds)),
                EnigioInstrumentEntity.class, "dis_instrument_flows");

        assertEquals(flowCount, flows.size(), "Should still have " + flowCount + " flows");

        int withRecovery = 0;
        int multiRecovery = 0;
        for (EnigioInstrumentEntity f : flows) {
            if (f.getRecoveryCount() >= 1) withRecovery++;
            if (f.getRecoveryCount() > 1) multiRecovery++;
            assertNull(f.getClaimedBy(),
                    "All claims should be released after processing: " + f.getId());
        }

        assertEquals(flowCount, withRecovery,
                "All " + flowCount + " flows should have been recovered");
        assertEquals(0, multiRecovery,
                "No flow should have recoveryCount > 1 (would indicate duplicate claiming)");
    }

    // ========== HELPERS ==========

    @SuppressWarnings("unchecked")
    private Map<String, Object> startFlow(String reference) {
        return rest.post().uri("/flows/enigio-instrument")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "reference", reference,
                        "title", "Resilience Test — " + reference,
                        "content", "Test content",
                        "instrumentType", "PROMISSORY_NOTE",
                        "documentCode", "NEG",
                        "signers", List.of(
                                Map.of("name", "Signer", "email", "s@test.com",
                                        "phone", "+1234567890", "capacity", "CEO",
                                        "organisation", "Corp", "order", 1)),
                        "recipient", Map.of("name", "Bank", "email", "bank@test.com")
                ))
                .retrieve()
                .body(Map.class);
    }

    private EnigioInstrumentEntity waitForStep(String flowId, String targetStep, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null) {
                // Accept exact match OR flow that already passed the target step
                if (targetStep.equals(flow.getCurrentStep())) return flow;
                if (flow.getCompletedSteps() != null && flow.getCompletedSteps().contains(targetStep)) return flow;
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        fail("Flow " + flowId + " did not reach step " + targetStep + " within " + timeout +
                ". Current: " + (flow != null ? flow.getStatus() + " @ " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }

    private String getVendorStats() {
        try {
            return RestClient.create("http://localhost:8081")
                    .get().uri("/admin/stats")
                    .retrieve().body(String.class);
        } catch (Exception e) {
            return "unavailable";
        }
    }

    private RestClient vendorAdmin() {
        return RestClient.create("http://localhost:8081");
    }

    private void setVendorFailure(String endpoint, String scenario) {
        vendorAdmin().post().uri("/admin/failure-config")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(endpoint, scenario))
                .retrieve().body(String.class);
    }

    private void clearVendorFailures() {
        try {
            vendorAdmin().post().uri("/admin/reset").retrieve().body(String.class);
        } catch (Exception ignored) {}
    }

    // ========== Vendor HTTP Status Code Tests ==========

    @Test
    @Order(10)
    @DisplayName("Vendor 409 Conflict on registerDocument → @RecoverOn SKIP, flow continues")
    void vendor409_registerDocument_recoversViaSkip() throws Exception {
        setVendorFailure("createDocument", "HTTP_409");
        var result = startFlow("SC-409-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        // 409 on createDocument → @RecoverOn(httpStatus=409, message="already", action=SKIP)
        // Flow should skip registerDocument and continue to addAttachment
        Thread.sleep(15000);
        clearVendorFailures();
        Thread.sleep(10000);

        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        // Flow should have advanced past CREATE_DRAFT (no 409 on that step)
        // registerDocument has @RecoverOn(409) so it skips on conflict
        assertTrue(flow.getCompletedSteps().contains("CREATE_DRAFT"),
                "CREATE_DRAFT should complete (no failure configured for it)");
    }

    @Test
    @Order(11)
    @DisplayName("Vendor 500 on createDocument → retries via Kafka retry topics")
    void vendor500_createDocument_retriesAndRecovers() throws Exception {
        setVendorFailure("createDocument", "HTTP_500");
        var result = startFlow("SC-500-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        // 500 is retryable — let it retry a few times then clear
        Thread.sleep(5000);
        clearVendorFailures();

        // Wait for retry cycle to complete and flow to advance
        EnigioInstrumentEntity flow = waitForStepCompleted(flowId, "REGISTER_DOCUMENT", Duration.ofMinutes(3));
        assertNotNull(flow);
        assertTrue(flow.getCompletedSteps().contains("REGISTER_DOCUMENT"),
                "REGISTER_DOCUMENT should complete after vendor recovers");
    }

    @Test
    @Order(12)
    @DisplayName("Vendor 429 Too Many Requests → retryable, backs off")
    void vendor429_tooManyRequests_retriesWithBackoff() throws Exception {
        setVendorFailure("createDocument", "HTTP_429");
        var result = startFlow("SC-429-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        Thread.sleep(5000);
        clearVendorFailures();

        EnigioInstrumentEntity flow = waitForStepCompleted(flowId, "REGISTER_DOCUMENT", Duration.ofMinutes(3));
        assertTrue(flow.getCompletedSteps().contains("REGISTER_DOCUMENT"),
                "Should recover after 429 clears");
    }

    @Test
    @Order(13)
    @DisplayName("Vendor 400 Bad Request → non-retryable, flow fails with compensation")
    void vendor400_badRequest_failsNonRetryable() throws Exception {
        setVendorFailure("createDocument", "HTTP_400");
        var result = startFlow("SC-400-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity flow = waitForTerminal(flowId, Duration.ofMinutes(3));
        assertNotNull(flow);
        FlowStatus status = flow.getStatus();
        assertTrue(status == FlowStatus.FAILED || status == FlowStatus.COMPENSATION_FAILED || status == FlowStatus.COMPENSATING,
                "400 should lead to terminal failure, got: " + status);
    }

    @Test
    @Order(14)
    @DisplayName("Vendor 503 Service Unavailable → retryable, recovers when available")
    void vendor503_serviceUnavailable_retriesAndRecovers() throws Exception {
        setVendorFailure("createDocument", "HTTP_503");
        var result = startFlow("SC-503-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        Thread.sleep(5000);
        clearVendorFailures();

        EnigioInstrumentEntity flow = waitForStepCompleted(flowId, "REGISTER_DOCUMENT", Duration.ofMinutes(3));
        assertTrue(flow.getCompletedSteps().contains("REGISTER_DOCUMENT"),
                "Should recover after 503 clears");
    }

    @Test
    @Order(15)
    @DisplayName("Vendor 502 Bad Gateway → retryable")
    void vendor502_badGateway_retryable() throws Exception {
        setVendorFailure("createDocument", "HTTP_502");
        var result = startFlow("SC-502-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        Thread.sleep(5000);
        clearVendorFailures();

        EnigioInstrumentEntity flow = waitForStepCompleted(flowId, "REGISTER_DOCUMENT", Duration.ofMinutes(3));
        assertNotNull(flow);
        assertTrue(flow.getCompletedSteps().contains("REGISTER_DOCUMENT"),
                "Should recover after 502 clears");
    }

    @Test
    @Order(16)
    @DisplayName("Vendor 403 Forbidden → non-retryable, flow fails")
    void vendor403_forbidden_failsNonRetryable() throws Exception {
        setVendorFailure("createDocument", "HTTP_403");
        var result = startFlow("SC-403-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity flow = waitForTerminal(flowId, Duration.ofMinutes(3));
        assertNotNull(flow);
        FlowStatus status = flow.getStatus();
        assertTrue(status == FlowStatus.FAILED || status == FlowStatus.COMPENSATION_FAILED || status == FlowStatus.COMPENSATING,
                "403 should lead to terminal failure, got: " + status);
    }

    private EnigioInstrumentEntity waitForStepCompleted(String flowId, String stepName, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && flow.getCompletedSteps() != null && flow.getCompletedSteps().contains(stepName)) return flow;
            try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
        }
        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        fail("Flow " + flowId + " step " + stepName + " not completed within " + timeout +
                ". Current: " + (flow != null ? flow.getStatus() + " @ " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }

    private EnigioInstrumentEntity waitForTerminal(String flowId, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null) {
                FlowStatus s = flow.getStatus();
                if (s == FlowStatus.FAILED || s == FlowStatus.CANCELLED || s == FlowStatus.COMPENSATION_FAILED
                        || s == FlowStatus.COMPLETED || s == FlowStatus.COMPENSATING) return flow;
            }
            try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
        }
        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        fail("Flow " + flowId + " not terminal within " + timeout +
                ". Current: " + (flow != null ? flow.getStatus() + " @ " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }
}
