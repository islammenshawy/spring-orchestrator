package com.dis.instrument;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.model.InstrumentType;
import com.dis.instrument.model.SigningStatus;
import com.orchestrator.starter.domain.FlowStatus;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for webhook event handling edge cases:
 * - Signing status race conditions (SIGNED vs REJECTED)
 * - Handler error isolation (no 500 to vendor)
 * - Concurrent webhook processing safety
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.context.ActiveProfiles("test")
class WebhookConcurrencyTest {

    @LocalServerPort
    private int port;

    @Autowired
    private MongoTemplate mongoTemplate;

    private RestClient rest;

    @BeforeEach
    void setUp() {
        rest = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultHeader("X-API-Key", "test-api-key").build();
    }

    // ========== DS-2.2: SignatureRejected must NOT overwrite SIGNED status ==========

    @Test
    @Order(1)
    @DisplayName("DS-2.2: SIGNATURE_REJECTED after FULLY_SIGNED does not overwrite SIGNED")
    void signatureRejected_afterSigned_doesNotOverwrite() {
        // Create a flow and manually set it to SIGNED
        var result = startFlow("DS-REJECT-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");
        waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofSeconds(30));

        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
        assertNotNull(flow);
        String traceId = flow.getTraceOriginalId();
        assertNotNull(traceId, "Flow should have traceOriginalId after preparation");

        // Set signing status to SIGNED (simulating FULLY_SIGNED webhook already processed)
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update().set("signingStatus", SigningStatus.SIGNED.name()),
                EnigioInstrumentEntity.class);

        // Now send SIGNATURE_REJECTED webhook — should NOT overwrite SIGNED
        rest.post().uri("/webhooks/enigio")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("traceOriginalId", traceId,
                        "eventType", "SIGNATURE_REJECTED",
                        "messageId", UUID.randomUUID().toString()))
                .retrieve().body(String.class);

        // Verify status is still SIGNED
        EnigioInstrumentEntity after = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
        assertNotNull(after);
        assertEquals(SigningStatus.SIGNED, after.getSigningStatus(),
                "SIGNATURE_REJECTED must not overwrite SIGNED status");
    }

    // ========== DS-3.4: Webhook handler exception returns 200, not 500 ==========

    @Test
    @Order(2)
    @DisplayName("DS-3.4: Webhook with invalid traceOriginalId returns 200 (not 500)")
    void webhook_handlerException_returns200() {
        // Send a webhook for a non-existent traceOriginalId — handler runs but finds no flow
        // This should NOT throw a 500
        String response = rest.post().uri("/webhooks/enigio")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("traceOriginalId", "nonexistent-trace-id-" + UUID.randomUUID(),
                        "eventType", "FULLY_SIGNED",
                        "messageId", UUID.randomUUID().toString()))
                .retrieve().body(String.class);

        assertNotNull(response);
        assertTrue(response.contains("received"), "Should return received status");
    }

    @Test
    @Order(3)
    @DisplayName("DS-3.4: Webhook with missing fields returns 400 (validation)")
    void webhook_missingFields_returns400() {
        try {
            rest.post().uri("/webhooks/enigio")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("eventType", "FULLY_SIGNED"))
                    .retrieve().body(String.class);
            fail("Should have returned 400");
        } catch (org.springframework.web.client.HttpClientErrorException.BadRequest e) {
            assertTrue(e.getResponseBodyAsString().contains("Missing traceOriginalId"));
        }
    }

    // ========== DS-2.2: Concurrent SIGNED + REJECTED webhooks ==========

    @Test
    @Order(4)
    @DisplayName("DS-2.2: Concurrent FULLY_SIGNED and SIGNATURE_REJECTED — SIGNED wins")
    void concurrentSignedAndRejected_signedWins() throws Exception {
        var result = startFlow("DS-CONC-" + UUID.randomUUID().toString().substring(0, 8));
        String flowId = (String) result.get("id");
        waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofSeconds(30));

        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
        assertNotNull(flow);
        String traceId = flow.getTraceOriginalId();
        assertNotNull(traceId);

        // Set to PARTIALLY_SIGNED first
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new Update().set("signingStatus", SigningStatus.PARTIALLY_SIGNED.name()),
                EnigioInstrumentEntity.class);

        // Fire FULLY_SIGNED and SIGNATURE_REJECTED concurrently
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(2);

        exec.submit(() -> {
            try { latch.await(); } catch (InterruptedException ignored) {}
            rest.post().uri("/webhooks/enigio")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("traceOriginalId", traceId,
                            "eventType", "FULLY_SIGNED",
                            "messageId", UUID.randomUUID().toString()))
                    .retrieve().body(String.class);
        });
        exec.submit(() -> {
            try { latch.await(); } catch (InterruptedException ignored) {}
            rest.post().uri("/webhooks/enigio")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("traceOriginalId", traceId,
                            "eventType", "SIGNATURE_REJECTED",
                            "messageId", UUID.randomUUID().toString()))
                    .retrieve().body(String.class);
        });

        latch.countDown(); // release both
        exec.shutdown();
        exec.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

        // Give MongoDB time to settle
        Thread.sleep(500);

        EnigioInstrumentEntity after = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
        assertNotNull(after);
        // SIGNED must win — REJECTED guard prevents overwrite
        assertEquals(SigningStatus.SIGNED, after.getSigningStatus(),
                "SIGNED must win over REJECTED in concurrent scenario");
    }

    // ========== Helpers ==========

    @SuppressWarnings("unchecked")
    private Map<String, Object> startFlow(String ref) {
        return rest.post().uri("/flows/enigio-instrument")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "correlationId", "ds-test-" + ref,
                        "reference", ref,
                        "title", "Deepseek Regression " + ref,
                        "instrumentType", InstrumentType.PROMISSORY_NOTE.name(),
                        "documentCode", "NEG",
                        "signers", java.util.List.of(Map.of(
                                "name", "Alice", "email", "alice@test.com",
                                "phone", "+46700000001", "capacity", "CEO",
                                "organisation", "Test AB", "order", 1)),
                        "recipient", Map.of("name", "Bob", "email", "bob@test.com")))
                .retrieve().body(Map.class);
    }

    private void waitForStep(String flowId, String stepName, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class);
            if (flow != null && stepName.equals(flow.getCurrentStep())) return;
            if (flow != null && flow.getStatus() == FlowStatus.COMPLETED) return;
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        }
        fail("Timed out waiting for step " + stepName + " on flow " + flowId);
    }
}
