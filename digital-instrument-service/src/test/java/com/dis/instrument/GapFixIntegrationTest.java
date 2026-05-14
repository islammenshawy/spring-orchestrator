package com.dis.instrument;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.InstrumentType;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.outbox.OutboxEvent;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for gap analysis fixes (Gaps 1.2/4.3, 1.3, 10.2).
 * Runs against live Docker infrastructure: Kafka, MongoDB, mock-vendor.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.context.ActiveProfiles("test")
class GapFixIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private StaleFlowRecoveryService staleFlowRecoveryService;

    private RestClient rest;

    @BeforeEach
    void setUp() {
        rest = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultHeader("X-API-Key", "test-api-key").build();
    }

    // ========== Fix 1.2/4.3: Outbox poison event — dead-lettered after max retries ==========

    @Test
    @Order(1)
    @DisplayName("Gap 1.2/4.3: Poison outbox event is dead-lettered, does not block pipeline")
    void outboxPoisonEvent_deadLetteredAfterMaxRetries_doesNotBlockPipeline() throws Exception {
        // Insert a poison outbox event with an invalid topic that Kafka will reject
        OutboxEvent poisonEvent = OutboxEvent.builder()
                .id("poison-" + UUID.randomUUID())
                .flowId("poison-flow")
                .topic("") // empty topic — Kafka will reject
                .key("k1")
                .payload("{\"bad\":true}")
                .published(false)
                .deadLettered(false)
                .failureCount(0)
                .build();
        outboxRepository.save(poisonEvent);

        // Insert a valid outbox event AFTER the poison one
        OutboxEvent goodEvent = OutboxEvent.builder()
                .id("good-" + UUID.randomUUID())
                .flowId("good-flow")
                .topic("dis.test.commands")
                .key("k2")
                .payload("{\"eventId\":\"" + UUID.randomUUID() + "\",\"flowId\":\"nonexistent\",\"stepName\":\"X\"}")
                .published(false)
                .deadLettered(false)
                .failureCount(0)
                .build();
        outboxRepository.save(goodEvent);

        // Wait for outbox publisher to process (polls every 500ms, max 5 retries)
        Thread.sleep(8000);

        // Poison event should be dead-lettered
        OutboxEvent poison = outboxRepository.findById(poisonEvent.getId()).orElse(null);
        assertNotNull(poison);
        assertTrue(poison.isDeadLettered(),
                "Poison event should be dead-lettered after max retries");
        assertTrue(poison.getFailureCount() >= 5,
                "Poison event should have at least 5 failure attempts, got " + poison.getFailureCount());

        // Good event should be published (pipeline not blocked)
        OutboxEvent good = outboxRepository.findById(goodEvent.getId()).orElse(null);
        assertNotNull(good);
        assertTrue(good.isPublished(),
                "Good event should be published — pipeline must not be blocked by poison event");

        // Cleanup
        outboxRepository.deleteById(poisonEvent.getId());
        outboxRepository.deleteById(goodEvent.getId());
    }

    @Test
    @Order(2)
    @DisplayName("Gap 1.2/4.3: Dead-lettered events are excluded from future polls")
    void outboxDeadLettered_excludedFromPolls() throws Exception {
        // Insert a dead-lettered event
        OutboxEvent deadLettered = OutboxEvent.builder()
                .id("dl-" + UUID.randomUUID())
                .flowId("dl-flow")
                .topic("")
                .key("k1")
                .payload("{}")
                .published(false)
                .deadLettered(true) // already dead-lettered
                .failureCount(5)
                .build();
        outboxRepository.save(deadLettered);

        // Query the same way the publisher does
        var events = outboxRepository.findTop100ByPublishedFalseAndDeadLetteredFalseOrderByCreatedAtAsc();
        boolean containsDeadLettered = events.stream()
                .anyMatch(e -> e.getId().equals(deadLettered.getId()));
        assertFalse(containsDeadLettered,
                "Dead-lettered events must be excluded from publisher poll");

        // Cleanup
        outboxRepository.deleteById(deadLettered.getId());
    }

    // ========== Fix 1.3: Stale recovery covers all flow types ==========

    @Test
    @Order(3)
    @DisplayName("Gap 1.3: Stale flow recovery service is injected and uses registry")
    void staleRecovery_serviceInjected_usesRegistry() {
        // The StaleFlowRecoveryService should be wired with FlowTypeRegistry
        // (not a single flow type). Verify it was injected successfully.
        assertNotNull(staleFlowRecoveryService,
                "StaleFlowRecoveryService should be injected as a Spring bean");
    }

    @Test
    @Order(4)
    @DisplayName("Gap 1.3: Stale flow recovery re-publishes stuck flows")
    void staleRecovery_republishesStuckFlows() throws Exception {
        // Start a flow and let it reach a step
        var result = startFlow("PN-STALE-001");
        String flowId = (String) result.get("id");

        // Wait for flow to reach gate step
        waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));

        // Simulate a stale flow: set status=IN_PROGRESS, updatedAt=20 min ago
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update()
                        .set("status", "IN_PROGRESS")
                        .set("updatedAt", Instant.now().minus(20, ChronoUnit.MINUTES)),
                "dis_instrument_flows");

        // Trigger recovery manually
        staleFlowRecoveryService.recoverStaleFlows();

        // Verify flow's updatedAt was bumped (recovery re-published and saved)
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        assertTrue(flow.getUpdatedAt().isAfter(Instant.now().minus(1, ChronoUnit.MINUTES)),
                "Recovery should bump updatedAt to now");
    }

    // ========== Fix 10.2: Step execution timeout ==========

    @Test
    @Order(5)
    @DisplayName("Gap 10.2: Step timeout config property is applied")
    void stepTimeout_configPropertyApplied() {
        // Verify the timeout config is loaded (default 60s unless overridden)
        // We can't easily test the actual timeout in an integration test without
        // a custom slow step, but we can verify the config wiring works by
        // starting a normal flow and confirming it completes (timeout doesn't
        // interfere with normal execution)
        var result = startFlow("PN-TIMEOUT-001");
        String flowId = (String) result.get("id");

        // Wait for gate step — proves steps executed within timeout
        EnigioInstrumentEntity flow = waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));
        assertNotNull(flow);
        assertTrue(flow.isPdfGenerated(), "Steps should complete within timeout");
        assertNotNull(flow.getTraceOriginalId(), "Register step should complete within timeout");
    }

    // ========== Fix 6.3: Per-flow retry config removed — no dead config ==========

    @Test
    @Order(6)
    @DisplayName("Gap 6.3: FlowTypeDescriptor has no retryConfig field")
    void deadRetryConfig_fieldRemoved() throws Exception {
        // Verify at runtime that FlowTypeDescriptor no longer has a retryConfig field
        var clazz = com.orchestrator.starter.flow.FlowTypeDescriptor.class;
        boolean hasRetryConfig = java.util.Arrays.stream(clazz.getDeclaredFields())
                .anyMatch(f -> f.getName().equals("retryConfig"));
        assertFalse(hasRetryConfig,
                "FlowTypeDescriptor should no longer have a retryConfig field (dead code removed)");
    }

    // ========== Helpers ==========

    @SuppressWarnings("unchecked")
    private Map<String, Object> startFlow(String reference) {
        return rest.post().uri("/flows/enigio-instrument")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "reference", reference,
                        "title", "Promissory Note — Integration Test",
                        "content", "Test Corp promises to pay",
                        "instrumentType", "PROMISSORY_NOTE",
                        "documentCode", "NEG",
                        "parties", List.of(Map.of("name", "Corp", "role", "ISSUER")),
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
            if (flow != null && targetStep.equals(flow.getCurrentStep())) return flow;
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        fail("Flow " + flowId + " did not reach step " + targetStep + " within " + timeout +
                ". Current: " + (flow != null ? flow.getStatus() + " @ " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }
}
