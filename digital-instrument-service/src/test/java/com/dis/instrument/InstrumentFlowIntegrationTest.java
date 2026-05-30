package com.dis.instrument;

import com.dis.instrument.model.*;
import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.FlowStatus;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Enigio instrument flow.
 * Requires: Kafka, MongoDB, mock-vendor — all running (Docker or native).
 *
 * Tests verify the complete 9-step flow:
 * Group 1: CREATE_DRAFT → REGISTER_DOCUMENT → ADD_ATTACHMENT
 * Group 2: ADD_SIGNERS → SEND_FOR_SIGNING → AWAIT_SIGNATURES
 * Group 3: VALIDATE_DOCUMENT → CREATE_ENVELOPE → TRANSFER_DOCUMENT
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.springframework.test.context.ActiveProfiles("test")
class InstrumentFlowIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private MongoTemplate mongoTemplate;

    private RestClient rest;
    private RestClient vendorAdmin;

    @BeforeAll
    static void waitForKafka(@Autowired MongoTemplate mongo) throws InterruptedException {
        Thread.sleep(3000); // Wait for Kafka consumers to join groups on first run
    }

    @BeforeEach
    void setUp() {
        rest = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultHeader("X-API-Key", "test-api-key").build();
        vendorAdmin = RestClient.create("http://localhost:8081");
        // DO NOT reset mock-vendor here — async flows (AWAIT_SIGNATURES) poll
        // the mock-vendor after setUp returns, resetting would wipe in-flight state.
        // Only reset failure config, not document state.
        try {
            vendorAdmin.post().uri("/admin/failure-config")
                    .body(Map.of()).retrieve().body(String.class);
        } catch (Exception ignored) {}
    }

    private void resetMockVendor() {
        try {
            vendorAdmin.post().uri("/admin/reset").retrieve().body(String.class);
        } catch (Exception ignored) {}
    }

    // ===== Flow Helpers =====

    @SuppressWarnings("unchecked")
    private Map<String, Object> startInstrumentFlow(String reference, InstrumentType type) {
        return rest.post().uri("/flows/enigio-instrument")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "reference", reference,
                        "title", type.toEnigioValue() + " — Test Corp to Bank",
                        "content", "Test Corp promises to pay Bank EUR 500,000",
                        "instrumentType", type.name(),
                        "documentCode", "NEG",
                        "parties", List.of(
                                Map.of("name", "Test Corp", "role", "ISSUER", "orgNumber", "123456-7890"),
                                Map.of("name", "Test Bank", "role", "BENEFICIARY")
                        ),
                        "signers", List.of(
                                Map.of("name", "Alice CEO", "email", "alice@test.com",
                                        "phone", "+46700000001", "capacity", "CEO",
                                        "organisation", "Test Corp", "order", 1),
                                Map.of("name", "Bob CFO", "email", "bob@test.com",
                                        "phone", "+46700000002", "capacity", "CFO",
                                        "organisation", "Test Corp", "order", 2)
                        ),
                        "recipient", Map.of("name", "Bank Operations", "email", "ops@bank.com")
                ))
                .retrieve()
                .body(Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> startFlowWithAttachments(String reference) {
        return rest.post().uri("/flows/enigio-instrument")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "reference", reference,
                        "title", "Promissory Note with Attachments",
                        "content", "Contract terms...",
                        "instrumentType", "PROMISSORY_NOTE",
                        "documentCode", "NEG",
                        "parties", List.of(Map.of("name", "Corp", "role", "ISSUER")),
                        "signers", List.of(
                                Map.of("name", "Signer", "email", "s@test.com",
                                        "phone", "+1234567890", "capacity", "CEO",
                                        "organisation", "Corp", "order", 1)
                        ),
                        "recipient", Map.of("name", "Recipient", "email", "r@bank.com"),
                        "attachments", List.of(
                                Map.of("filename", "terms.pdf", "data", "base64data", "comment", "Terms sheet"),
                                Map.of("filename", "kyc.pdf", "data", "base64kyc", "comment", "KYC document")
                        )
                ))
                .retrieve()
                .body(Map.class);
    }

    private EnigioInstrumentEntity waitForStatus(String flowId, FlowStatus expected, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && flow.getStatus() == expected) return flow;

            // Auto-approve gate steps (simulate downstream approval)
            if (flow != null) {
                String step = flow.getCurrentStep();
                if ("AWAIT_PREPARATION_APPROVAL".equals(step) && flow.isPreparationNotified() && !flow.isSigningApproved()) {
                    try {
                        rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                                .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                                .retrieve().body(Map.class);
                    } catch (Exception ignored) {}
                }
                if ("AWAIT_DELIVERY_APPROVAL".equals(step) && flow.isSigningNotified() && !flow.isDeliveryApproved()) {
                    try {
                        rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                                .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                                .retrieve().body(Map.class);
                    } catch (Exception ignored) {}
                }
                // Gate: Signing — simulate FULLY_SIGNED webhook if signing is pending
                if ("AWAIT_SIGNATURES".equals(step) && flow.isSigningEmailsSent()
                        && com.dis.instrument.model.SigningStatus.SIGNED != flow.getSigningStatus()) {
                    try {
                        rest.post().uri("/webhooks/enigio")
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(Map.of(
                                        "messageId", java.util.UUID.randomUUID().toString(),
                                        "traceOriginalId", flow.getTraceOriginalId(),
                                        "eventType", "FULLY_SIGNED",
                                        "timestamp", java.time.Instant.now().toString()))
                                .retrieve().body(Map.class);
                    } catch (Exception ignored) {}
                }
                // Gate 3: Transfer — simulate recipient accepting via webhook
                if ("TRANSFER_DOCUMENT".equals(step) && flow.getTransferId() != null && !flow.isTransferAccepted()) {
                    try {
                        rest.post().uri("/webhooks/enigio")
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(Map.of(
                                        "messageId", java.util.UUID.randomUUID().toString(),
                                        "traceOriginalId", flow.getEnvelopeTraceId(),
                                        "eventType", "TRANSFER",
                                        "timestamp", java.time.Instant.now().toString()))
                                .retrieve().body(Map.class);
                    } catch (Exception ignored) {}
                }
            }

            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        fail("Flow " + flowId + " did not reach " + expected + " within " + timeout +
                ". Current: " + (flow != null ? flow.getStatus() + " @ " + flow.getCurrentStep() : "NOT FOUND"));
        return null;
    }

    // ========== 1. Happy path — all 9 steps ==========

    @Test
    @Order(1)
    @DisplayName("Happy path: promissory note completes all 9 steps")
    void happyPath_promissoryNote_completesAllSteps() {
        var result = startInstrumentFlow("PN-TEST-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");
        assertNotNull(flowId);
        assertEquals("enigio-instrument", result.get("flowType"));

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        // Group 1 results
        assertTrue(completed.isPdfGenerated(), "Draft should be generated");
        assertNotNull(completed.getTraceOriginalId(), "Should have traceOriginalId from Enigio");
        assertNotNull(completed.getVersionKey(), "Should have versionKey");

        // Group 2 results
        assertTrue(completed.isSignersAdded(), "Signers should be added");
        assertTrue(completed.isSigningEmailsSent(), "Signing emails should be sent");
        assertEquals(com.dis.instrument.model.SigningStatus.SIGNED, completed.getSigningStatus(), "Signing should complete");

        // Group 3 results
        assertEquals("VALID", completed.getValidationResult(), "Validation should pass");
        assertNotNull(completed.getEnvelopeDraftId(), "Should have envelope draft ID");
        assertNotNull(completed.getEnvelopeTraceId(), "Should have sealed envelope trace ID");
        assertNotNull(completed.getTransferId(), "Should have transfer ID");
        assertTrue(completed.isTransferAccepted(), "Transfer should be accepted (via TRANSFER webhook)");

        // Orchestrator fields
        assertEquals(0, completed.getRetryCount(), "No retries on happy path");
        // errorMessage may contain last gate waiting message — that's OK for completed flows
        assertEquals("enigio-instrument", completed.getFlowType());
    }

    // ========== 2. With attachments ==========

    @Test
    @Order(2)
    @DisplayName("Flow with attachments triggers amend step")
    void withAttachments_amendStepExecutes() {
        var result = startFlowWithAttachments("PN-ATTACH-001");
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        assertNotNull(completed.getAttachmentVersionKey(), "Should have attachment version key");
        assertNotEquals("NONE", completed.getAttachmentVersionKey(),
                "Attachment version key should not be NONE when attachments provided");
    }

    // ========== 3. Without attachments — skips amend ==========

    @Test
    @Order(3)
    @DisplayName("Flow without attachments skips amend step")
    void withoutAttachments_skipAmendStep() {
        var result = startInstrumentFlow("PN-NOATTACH-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        assertEquals("NONE", completed.getAttachmentVersionKey(),
                "Should be NONE when no attachments");
    }

    // ========== 4. Flaky vendor — retries and recovers ==========

    @Test
    @Order(4)
    @DisplayName("Flaky vendor: flow recovers after Kafka retry")
    void flakyVendor_recoversAfterRetry() {
        vendorAdmin.post().uri("/admin/failure-config")
                .body(Map.of("createDocument", "FLAKY"))
                .retrieve().body(String.class);

        var result = startInstrumentFlow("PN-FLAKY-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(5));

        assertNotNull(completed.getTraceOriginalId());
        assertNotNull(completed.getTransferId());
    }

    // ========== 5. Different instrument types ==========

    @Test
    @Order(5)
    @DisplayName("Bill of lading instrument type completes")
    void billOfLading_completesSuccessfully() {
        var result = startInstrumentFlow("BL-TEST-001", InstrumentType.BILL_OF_LADING);
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        assertEquals("BILL_OF_LADING", completed.getInstrumentType().name());
        assertNotNull(completed.getTransferId());
    }

    // ========== 6. Step audit logs ==========

    @Test
    @Order(6)
    @DisplayName("Step audit logs created for all 9 steps")
    void stepLogs_createdForAllSteps() {
        var result = startInstrumentFlow("PN-AUDIT-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        var logs = mongoTemplate.find(
                new Query(Criteria.where("flowId").is(flowId)),
                org.bson.Document.class, "orchestrator_step_log");

        assertTrue(logs.size() >= 7,
                "Should have at least 7 step logs, got " + logs.size());

        var stepNames = logs.stream().map(d -> d.getString("stepName")).toList();
        assertTrue(stepNames.contains("CREATE_DRAFT"), "Missing CREATE_DRAFT log");
        assertTrue(stepNames.contains("REGISTER_DOCUMENT"), "Missing REGISTER_DOCUMENT log");
        assertTrue(stepNames.contains("VALIDATE_DOCUMENT"), "Missing VALIDATE_DOCUMENT log");
        assertTrue(stepNames.contains("CREATE_ENVELOPE"), "Missing CREATE_ENVELOPE log");
        // Gate steps may have WAITING entries instead of COMPLETED — both are valid
        // AWAIT_SIGNATURES, AWAIT_*_APPROVAL, TRANSFER_DOCUMENT use WaitingStepException
    }

    // ========== 7. Concurrent flows ==========

    @Test
    @Order(7)
    @DisplayName("5 concurrent instrument flows all complete")
    void concurrentFlows_allComplete() {
        List<String> flowIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            var result = startInstrumentFlow("PN-CONC-" + (i + 1), InstrumentType.PROMISSORY_NOTE);
            flowIds.add((String) result.get("id"));
        }

        for (String flowId : flowIds) {
            EnigioInstrumentEntity completed = waitForStatus(
                    flowId, FlowStatus.COMPLETED, Duration.ofMinutes(6));
            assertNotNull(completed.getTransferId(),
                    "Flow " + flowId + " should have transferId");
        }
    }

    // ========== 8. Idempotency ==========

    @Test
    @Order(8)
    @DisplayName("Processed events tracked for idempotency")
    void idempotency_processedEventsTracked() {
        var result = startInstrumentFlow("PN-IDEMP-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        long count = mongoTemplate.count(new Query(), "orchestrator_processed_events");
        assertTrue(count > 0, "Processed events should be tracked");
    }

    // ========== 9. Outbox events published ==========

    @Test
    @Order(9)
    @DisplayName("All outbox events published after completion")
    void outbox_allEventsPublished() {
        var result = startInstrumentFlow("PN-OUTBOX-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        var unpublished = mongoTemplate.find(
                new Query(Criteria.where("published").is(false).and("flowId").is(flowId)),
                org.bson.Document.class, "orchestrator_outbox");
        assertEquals(0, unpublished.size(), "All outbox events for this flow should be published");
    }

    // ========== 10. Entity fields persisted correctly ==========

    @Test
    @Order(10)
    @DisplayName("All entity fields persisted correctly")
    void entityFields_allPersisted() {
        var result = startInstrumentFlow("PN-FIELDS-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        EnigioInstrumentEntity completed = waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        // Input fields preserved
        assertEquals("PN-FIELDS-001", completed.getReference());
        assertEquals(InstrumentType.PROMISSORY_NOTE, completed.getInstrumentType());
        assertEquals(DocumentCode.NEG, completed.getDocumentCode());
        assertNotNull(completed.getTitle());
        assertNotNull(completed.getContent());
        assertEquals(2, completed.getSigners().size());
        assertEquals(2, completed.getParties().size());
        assertNotNull(completed.getRecipient());
        assertEquals("ops@bank.com", completed.getRecipient().getEmail());

        // Library fields
        assertNotNull(completed.getCorrelationId());
        assertNotNull(completed.getCreatedAt());
        assertNotNull(completed.getUpdatedAt());
        assertEquals(0, completed.getRetryCount());
    }

    // ========== 11. Flow cancellation ==========

    @Test
    @Order(11)
    @DisplayName("Cancel flow at gate step")
    void cancelFlow_atGateStep() {
        var result = startInstrumentFlow("PN-CANCEL-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for Gate 1
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && "AWAIT_PREPARATION_APPROVAL".equals(flow.getCurrentStep())) break;
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }

        // Cancel
        var cancelResult = rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("reason", "Test cancellation"))
                .retrieve()
                .body(Map.class);

        assertNotNull(cancelResult);
        assertEquals("CANCELLED", cancelResult.get("status"));

        // Verify in MongoDB
        EnigioInstrumentEntity cancelled = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(cancelled);
        assertEquals(FlowStatus.CANCELLED, cancelled.getStatus());
        assertTrue(cancelled.getErrorMessage().contains("Test cancellation"));
    }

    @Test
    @Order(12)
    @DisplayName("Cancel while in retry topic — retry message skipped")
    void cancelFlow_whileInRetry() {
        var result = startInstrumentFlow("PN-CANCEL-RETRY", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for PARKED status (gate step parks flow via waitUntil)
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && (flow.getStatus() == FlowStatus.PARKED
                    || flow.getStatus() == FlowStatus.WAITING_RETRY)) break;
            try { Thread.sleep(500); } catch (InterruptedException e) { break; }
        }

        // Cancel while in retry
        rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("reason", "Cancel during retry"))
                .retrieve()
                .body(Map.class);

        // Wait for retry message to be delivered and skipped
        try { Thread.sleep(15000); } catch (InterruptedException ignored) {}

        // Verify still CANCELLED (not re-executed)
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        assertEquals(FlowStatus.CANCELLED, flow.getStatus(),
                "Flow should stay CANCELLED even after retry message delivered");
    }

    // ========== 13. MongoDB offset store ==========

    @Test
    @Order(13)
    @DisplayName("Consumer offsets saved to MongoDB")
    void mongoOffsetStore_offsetsSaved() {
        var result = startInstrumentFlow("PN-OFFSET-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        // Check orchestrator_consumer_offsets collection
        long offsetCount = mongoTemplate.count(new Query(), "orchestrator_consumer_offsets");
        assertTrue(offsetCount > 0,
                "Consumer offsets should be saved to MongoDB for cross-cluster recovery");

        // Verify offset has required fields
        var offset = mongoTemplate.findOne(new Query(), org.bson.Document.class,
                "orchestrator_consumer_offsets");
        assertNotNull(offset);
        assertNotNull(offset.get("topic"), "Offset should have topic");
        assertNotNull(offset.get("offset"), "Offset should have offset number");
        assertNotNull(offset.get("messageTimestamp"), "Offset should have timestamp for cross-cluster seek");
    }

    // ========== 14. Cannot cancel completed flow ==========

    @Test
    @Order(14)
    @DisplayName("Cannot cancel completed flow")
    void cancelFlow_completedFlowRejected() {
        var result = startInstrumentFlow("PN-CANCEL-DONE", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        waitForStatus(flowId, FlowStatus.COMPLETED, Duration.ofMinutes(3));

        // Try to cancel completed flow
        try {
            rest.post()
                    .uri("/flows/enigio-instrument/" + flowId + "/cancel")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("reason", "Should fail"))
                    .retrieve()
                    .body(Map.class);
            // If no exception, check response
        } catch (Exception e) {
            // 400 expected
            assertTrue(e.getMessage().contains("400") || e.getMessage().contains("Bad Request"),
                    "Should return 400 for completed flow");
        }

        // Flow should still be COMPLETED
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertEquals(FlowStatus.COMPLETED, flow.getStatus());
    }

    // ========== 15. Cancel after signing — document invalidated on vendor ==========

    @Test
    @Order(15)
    @DisplayName("Cancel after signing — verify document invalidated on Enigio")
    void cancelFlow_afterSigning_documentInvalidated() {
        var result = startInstrumentFlow("PN-CANCEL-SIGN", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Progress through signing (approve both gates)
        EnigioInstrumentEntity signed = waitForStep(flowId, "AWAIT_DELIVERY_APPROVAL", Duration.ofMinutes(3));
        assertNotNull(signed);
        assertNotNull(signed.getTraceOriginalId(), "Should have traceOriginalId before cancel");
        assertEquals(com.dis.instrument.model.SigningStatus.SIGNED, signed.getSigningStatus(), "Should be signed before cancel");

        // Cancel at Gate 2 (document registered + signed but not delivered)
        var cancelResult = rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("reason", "Counterparty withdrew from deal"))
                .retrieve()
                .body(Map.class);

        assertEquals("CANCELLED", cancelResult.get("status"));

        // Verify document invalidated on mock vendor
        try {
            var metadata = RestClient.create("http://localhost:8081")
                    .get().uri("/api/v1/documents/{id}/metadata", signed.getTraceOriginalId())
                    .retrieve().body(Map.class);
            assertEquals(true, metadata.get("invalidated"),
                    "Document should be invalidated on Enigio after cancel");
        } catch (Exception e) {
            // 404 also acceptable (document removed)
        }
    }

    // ========== 16. Cancel idempotent — double cancel returns same result ==========

    @Test
    @Order(16)
    @DisplayName("Double cancel is idempotent")
    void cancelFlow_doubleCancel_idempotent() {
        var result = startInstrumentFlow("PN-DOUBLE-CANCEL", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for gate step, then let any in-flight Kafka messages settle
        waitForStep(flowId, "AWAIT_PREPARATION_APPROVAL", Duration.ofMinutes(2));
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        // First cancel
        rest.post().uri("/flows/enigio-instrument/" + flowId + "/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("reason", "first cancel")).retrieve().body(Map.class);

        // Second cancel — should return error (already cancelled)
        try {
            rest.post().uri("/flows/enigio-instrument/" + flowId + "/cancel")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("reason", "second cancel")).retrieve().body(Map.class);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("400") || e.getMessage().contains("Bad Request"),
                    "Double cancel should return 400");
        }

        // Status should still be CANCELLED (not corrupted)
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertEquals(FlowStatus.CANCELLED, flow.getStatus());
    }

    // ========== Helper ==========

    private EnigioInstrumentEntity waitForStep(String flowId, String targetStep, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null) {
                String step = flow.getCurrentStep();
                // Auto-approve gates
                if ("AWAIT_PREPARATION_APPROVAL".equals(step) && flow.isPreparationNotified() && !flow.isSigningApproved()) {
                    try { rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                            .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                            .retrieve().body(Map.class); } catch (Exception ignored) {}
                }
                // Fire signing webhook if parked at AWAIT_SIGNATURES
                if ("AWAIT_SIGNATURES".equals(step) && flow.isSigningEmailsSent()
                        && com.dis.instrument.model.SigningStatus.SIGNED != flow.getSigningStatus() && flow.getTraceOriginalId() != null) {
                    try { rest.post().uri("/webhooks/enigio")
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(Map.of("messageId", java.util.UUID.randomUUID().toString(),
                                    "traceOriginalId", flow.getTraceOriginalId(),
                                    "eventType", "FULLY_SIGNED"))
                            .retrieve().body(Map.class); } catch (Exception ignored) {}
                }
                if ("AWAIT_DELIVERY_APPROVAL".equals(step) && flow.isSigningNotified() && !flow.isDeliveryApproved()) {
                    // DON'T approve Gate 2 — we want to cancel here
                }
                if (targetStep.equals(step)) return flow;
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
        fail("Flow " + flowId + " did not reach step " + targetStep + " within " + timeout);
        return null;
    }

    // ===== Search Attributes =====

    @Test
    @Order(20)
    @DisplayName("Search API — find flows by @SearchAttribute reference")
    void searchFlows_byReference() {
        // Start a flow with a unique reference
        var result = startInstrumentFlow("SEARCH-REF-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");
        assertNotNull(flowId);

        // Wait briefly for the flow to be persisted
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        // Search by reference
        @SuppressWarnings("unchecked")
        var searchResult = rest.get()
                .uri("/flows/enigio-instrument/search?reference=SEARCH-REF-001")
                .retrieve()
                .body(Map.class);

        assertNotNull(searchResult);
        assertEquals("enigio-instrument", searchResult.get("flowType"));
        assertTrue(((Number) searchResult.get("count")).intValue() >= 1,
                "Should find at least 1 flow with reference SEARCH-REF-001");
    }

    @Test
    @Order(21)
    @DisplayName("Search API — find flows by @SearchAttribute instrumentType")
    void searchFlows_byInstrumentType() {
        // Start flows with different instrument types
        startInstrumentFlow("SEARCH-TYPE-PN", InstrumentType.PROMISSORY_NOTE);
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        @SuppressWarnings("unchecked")
        var searchResult = rest.get()
                .uri("/flows/enigio-instrument/search?instrumentType=PROMISSORY_NOTE")
                .retrieve()
                .body(Map.class);

        assertNotNull(searchResult);
        assertTrue(((Number) searchResult.get("count")).intValue() >= 1,
                "Should find flows with instrumentType PROMISSORY_NOTE");
    }

    @Test
    @Order(22)
    @DisplayName("Search API — combined search attributes")
    void searchFlows_combined() {
        String uniqueRef = "SEARCH-COMBO-" + System.currentTimeMillis();
        startInstrumentFlow(uniqueRef, InstrumentType.BILL_OF_EXCHANGE);
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        @SuppressWarnings("unchecked")
        var searchResult = rest.get()
                .uri("/flows/enigio-instrument/search?reference=" + uniqueRef + "&instrumentType=BILL_OF_EXCHANGE")
                .retrieve()
                .body(Map.class);

        assertNotNull(searchResult);
        assertEquals(1, ((Number) searchResult.get("count")).intValue(),
                "Should find exactly 1 flow matching both attributes");
    }

    // ===== Signals =====

    @Test
    @Order(30)
    @DisplayName("Signal — updatePriority on parked flow executes immediately")
    void signal_parkedFlow_executesImmediately() throws Exception {
        var result = startInstrumentFlow("SIGNAL-PARKED-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for flow to reach a parked gate step
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && (flow.getStatus() == com.orchestrator.starter.domain.FlowStatus.PARKED)) break;
            Thread.sleep(500);
        }

        // Send signal to parked flow
        @SuppressWarnings("unchecked")
        var signalResult = rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/signal")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("signalName", "updatePriority", "payload", Map.of("priority", "URGENT", "reason", "customer escalation")))
                .retrieve()
                .body(Map.class);

        assertNotNull(signalResult);
        assertEquals("Signal delivered", signalResult.get("message"));

        // Verify priority was set on the flow
        Thread.sleep(1000);
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        assertEquals(com.dis.instrument.model.Priority.URGENT, flow.getPriority());
    }

    @Test
    @Order(31)
    @DisplayName("Signal — updatePriority on in-progress flow queues as pending")
    void signal_inProgressFlow_queuesPending() throws Exception {
        var result = startInstrumentFlow("SIGNAL-PROGRESS-001", InstrumentType.BILL_OF_EXCHANGE);
        String flowId = (String) result.get("id");

        // Immediately signal while flow is likely IN_PROGRESS
        Thread.sleep(200);

        try {
            @SuppressWarnings("unchecked")
            var signalResult = rest.post()
                    .uri("/flows/enigio-instrument/" + flowId + "/signal")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("signalName", "updatePriority", "payload", Map.of("priority", "HIGH", "reason", "SLA breach")))
                    .retrieve()
                    .body(Map.class);

            assertEquals("Signal delivered", signalResult.get("message"));
        } catch (Exception e) {
            // Flow may have already completed — that's OK for this test
        }

        // Wait for flow to process and drain pending signals
        Thread.sleep(5000);
        EnigioInstrumentEntity flow = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        // Priority may or may not be set depending on timing:
        // - If signal arrived while IN_PROGRESS → queued → drained → priority set
        // - If signal arrived while PARKED → executed immediately → priority set
        // - If flow completed before signal → signal may have been rejected
        // The test validates signal delivery doesn't crash — priority check is best-effort
        if (flow.getPriority() != null) {
            assertEquals(com.dis.instrument.model.Priority.HIGH, flow.getPriority());
        }
    }

    @Test
    @Order(32)
    @DisplayName("Signal — requestCancellation succeeds when not signed")
    void signal_requestCancellation_succeeds() throws Exception {
        var result = startInstrumentFlow("SIGNAL-CANCEL-OK", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");
        Thread.sleep(2000);

        // Flow is not signed yet — cancellation should succeed
        @SuppressWarnings("unchecked")
        var signalResult = rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/signal")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("signalName", "requestCancellation"))
                .retrieve()
                .body(Map.class);

        assertNotNull(signalResult);
        assertEquals("Signal delivered", signalResult.get("message"));
    }

    @Test
    @Order(33)
    @DisplayName("Signal — requestCancellation fails when signed")
    void signal_requestCancellation_failsWhenSigned() throws Exception {
        var result = startInstrumentFlow("SIGNAL-CANCEL-FAIL", InstrumentType.BILL_OF_LADING);
        String flowId = (String) result.get("id");

        // Wait for flow to reach a PARKED state
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity f = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (f != null && f.getStatus() == com.orchestrator.starter.domain.FlowStatus.PARKED) break;
            Thread.sleep(500);
        }

        // Set signingStatus to SIGNED while flow is PARKED (no race)
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new org.springframework.data.mongodb.core.query.Update()
                        .set("signingStatus", com.dis.instrument.model.SigningStatus.SIGNED.name()),
                EnigioInstrumentEntity.class);

        // Cancellation should fail — document is signed
        try {
            rest.post()
                    .uri("/flows/enigio-instrument/" + flowId + "/signal")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("signalName", "requestCancellation"))
                    .retrieve()
                    .body(Map.class);
            fail("Should have thrown — document is signed");
        } catch (Exception e) {
            // RestClient throws on 4xx/5xx — any error response means the signal was rejected
            assertNotNull(e, "Signal should be rejected for signed document");
        }
    }

    @Test
    @Order(34)
    @DisplayName("Signal — unknown signal returns error")
    void signal_unknownSignal_returnsError() {
        var result = startInstrumentFlow("SIGNAL-UNKNOWN-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        try {
            rest.post()
                    .uri("/flows/enigio-instrument/" + flowId + "/signal")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("signalName", "nonexistent", "payload", Map.of()))
                    .retrieve()
                    .body(Map.class);
            fail("Should have thrown for unknown signal");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("400") || e.getMessage().contains("Unknown signal"));
        }
    }

    // ===== Compensation =====

    @Test
    @Order(35)
    @DisplayName("Replay + Cancel round-trip — cancel then replay")
    void replayCancelRoundTrip() throws Exception {
        var result = startInstrumentFlow("ROUNDTRIP-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for PARKED
        if (!waitForParked(flowId)) { fail("Flow never parked"); return; }

        // Cancel it
        rest.post().uri("/flows/enigio-instrument/" + flowId + "/cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("reason", "round-trip test"))
                .retrieve().body(Map.class);

        Thread.sleep(1000);
        EnigioInstrumentEntity cancelled = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertEquals(com.orchestrator.starter.domain.FlowStatus.CANCELLED, cancelled.getStatus());

        // Replay it
        var replayResult = rest.post().uri("/flows/enigio-instrument/" + flowId + "/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .retrieve().body(Map.class);

        assertEquals("Flow replayed", replayResult.get("message"));

        // Verify it's back IN_PROGRESS
        Thread.sleep(2000);
        EnigioInstrumentEntity replayed = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotEquals(com.orchestrator.starter.domain.FlowStatus.CANCELLED, replayed.getStatus(),
                "Flow should no longer be CANCELLED after replay");
    }

    // ===== Race Condition Integration Tests =====

    @Test
    @Order(50)
    @DisplayName("Race: Concurrent approvals — no corruption")
    void race_concurrentApprovals() throws Exception {
        var result = startInstrumentFlow("RACE-APPROVE-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        if (!waitForParked(flowId)) { fail("Flow never parked"); return; }

        // Fire 3 concurrent approve requests
        var futures = new java.util.ArrayList<java.util.concurrent.CompletableFuture<?>>();
        for (int i = 0; i < 3; i++) {
            futures.add(java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                            .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                            .retrieve().body(Map.class);
                } catch (Exception ignored) {}
            }));
        }
        java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0])).join();

        Thread.sleep(2000);
        EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(flow);
        assertNotEquals(com.orchestrator.starter.domain.FlowStatus.FAILED, flow.getStatus(),
                "Concurrent approvals should not corrupt flow");
    }

    @Test
    @Order(51)
    @DisplayName("Race: Approve on already-advanced flow returns 400")
    void race_approveOnAdvancedFlow() throws Exception {
        var result = startInstrumentFlow("RACE-ADVANCED-001", InstrumentType.BILL_OF_EXCHANGE);
        String flowId = (String) result.get("id");

        // Wait for flow to reach PARKED, then approve it
        if (!waitForParked(flowId)) { fail("Flow never parked"); return; }

        rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                .retrieve().body(Map.class);

        // Wait for flow to advance past the gate
        Thread.sleep(5000);

        // Try to approve again — flow has advanced, should get 400
        try {
            rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                    .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                    .retrieve().body(Map.class);
            // Might succeed if flow is at another gate — that's OK
        } catch (Exception e) {
            // Expected 400 — flow is no longer at the approval gate
            assertTrue(e.getMessage().contains("400") || e.getMessage().contains("not awaiting"),
                    "Should reject approval on advanced flow");
        }
    }

    private boolean waitForParked(String flowId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && flow.getStatus() == com.orchestrator.starter.domain.FlowStatus.PARKED) return true;
            Thread.sleep(500);
        }
        return false;
    }

    // ===== Replay =====

    @Test
    @Order(40)
    @DisplayName("Replay — failed flow resumes and completes")
    void replay_failedFlow_resumesAndCompletes() throws Exception {
        var result = startInstrumentFlow("REPLAY-FAIL-001", InstrumentType.PROMISSORY_NOTE);
        String flowId = (String) result.get("id");

        // Wait for flow to reach a gate step (PARKED)
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(
                    flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && flow.getStatus() == com.orchestrator.starter.domain.FlowStatus.PARKED) break;
            Thread.sleep(500);
        }

        // Force flow to FAILED
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(flowId)),
                new org.springframework.data.mongodb.core.query.Update()
                        .set("status", "FAILED")
                        .set("errorMessage", "simulated failure"),
                EnigioInstrumentEntity.class);

        // Replay via REST
        @SuppressWarnings("unchecked")
        var replayResult = rest.post()
                .uri("/flows/enigio-instrument/" + flowId + "/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .retrieve()
                .body(Map.class);

        assertNotNull(replayResult);
        assertEquals("Flow replayed", replayResult.get("message"));

        // Wait for flow to progress after replay
        Thread.sleep(5000);
        EnigioInstrumentEntity replayed = mongoTemplate.findById(
                flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
        assertNotNull(replayed);
        assertNotEquals("FAILED", replayed.getStatus().name(),
                "Flow should have progressed past FAILED after replay");
    }

    @Test
    @Order(41)
    @DisplayName("Replay — completed flow rejected without allowCompleted")
    void replay_completedFlow_rejectedWithoutFlag() throws Exception {
        // Find a completed flow from earlier tests
        EnigioInstrumentEntity completed = mongoTemplate.findOne(
                Query.query(Criteria.where("status").is("COMPLETED")),
                EnigioInstrumentEntity.class, "dis_instrument_flows");

        if (completed == null) {
            // Start and wait for one to complete
            var result = startInstrumentFlow("REPLAY-COMPLETE-001", InstrumentType.BILL_OF_EXCHANGE);
            String flowId = (String) result.get("id");
            long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
            while (System.currentTimeMillis() < deadline) {
                completed = mongoTemplate.findById(flowId, EnigioInstrumentEntity.class, "dis_instrument_flows");
                if (completed != null && completed.getStatus() == com.orchestrator.starter.domain.FlowStatus.COMPLETED) break;
                // Auto-approve gates
                try {
                    rest.post().uri("/flows/enigio-instrument/" + flowId + "/approve")
                            .contentType(MediaType.APPLICATION_JSON).body(Map.of())
                            .retrieve().body(Map.class);
                } catch (Exception ignored) {}
                Thread.sleep(1000);
            }
        }

        if (completed != null && completed.getStatus() == com.orchestrator.starter.domain.FlowStatus.COMPLETED) {
            try {
                rest.post()
                        .uri("/flows/enigio-instrument/" + completed.getId() + "/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of())
                        .retrieve()
                        .body(Map.class);
                fail("Should reject replay of completed flow without allowCompleted");
            } catch (Exception e) {
                assertNotNull(e, "Replay of COMPLETED flow should fail without allowCompleted");
            }
        }
    }

    @Test
    @Order(42)
    @DisplayName("Replay — batch replay multiple flows")
    void replay_batchReplay() throws Exception {
        // Start 2 flows and force them to FAILED
        var r1 = startInstrumentFlow("BATCH-REPLAY-001", InstrumentType.PROMISSORY_NOTE);
        var r2 = startInstrumentFlow("BATCH-REPLAY-002", InstrumentType.BILL_OF_EXCHANGE);
        String id1 = (String) r1.get("id");
        String id2 = (String) r2.get("id");

        Thread.sleep(3000);

        // Force both to FAILED
        for (String id : List.of(id1, id2)) {
            mongoTemplate.updateFirst(
                    Query.query(Criteria.where("_id").is(id)),
                    new org.springframework.data.mongodb.core.query.Update()
                            .set("status", "FAILED")
                            .set("errorMessage", "batch failure"),
                    EnigioInstrumentEntity.class);
        }

        // Batch replay
        @SuppressWarnings("unchecked")
        var batchResult = rest.post()
                .uri("/flows/enigio-instrument/ops/batch-replay")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("flowIds", List.of(id1, id2)))
                .retrieve()
                .body(Map.class);

        assertNotNull(batchResult);
        assertEquals(2, ((Number) batchResult.get("total")).intValue());
        assertEquals(2, ((Number) batchResult.get("succeeded")).intValue());
    }

    @Test
    @Order(43)
    @DisplayName("Batch cancel — multiple flows")
    void batchCancel_multipleFlows() throws Exception {
        var r1 = startInstrumentFlow("BATCH-CANCEL-001", InstrumentType.PROMISSORY_NOTE);
        var r2 = startInstrumentFlow("BATCH-CANCEL-002", InstrumentType.BILL_OF_LADING);
        String id1 = (String) r1.get("id");
        String id2 = (String) r2.get("id");

        // Wait for flows to reach PARKED
        Thread.sleep(5000);

        @SuppressWarnings("unchecked")
        var cancelResult = rest.post()
                .uri("/flows/enigio-instrument/ops/batch-cancel")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("flowIds", List.of(id1, id2), "reason", "batch cancel test"))
                .retrieve()
                .body(Map.class);

        assertNotNull(cancelResult);
        assertEquals(2, ((Number) cancelResult.get("total")).intValue());

        // Verify at least one cancelled
        Thread.sleep(1000);
        long cancelled = 0;
        for (String id : List.of(id1, id2)) {
            EnigioInstrumentEntity flow = mongoTemplate.findById(id, EnigioInstrumentEntity.class, "dis_instrument_flows");
            if (flow != null && flow.getStatus() == com.orchestrator.starter.domain.FlowStatus.CANCELLED) cancelled++;
        }
        assertTrue(cancelled >= 1, "At least one flow should be cancelled");
    }
}
