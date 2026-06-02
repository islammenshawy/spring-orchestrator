package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.FlowEndpointAutoConfiguration.FlowEndpointController;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.flow.ReplayOptions;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FlowEndpointAutoConfiguration$FlowEndpointController.
 * Tests controller methods directly with mocked dependencies.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({"unchecked", "rawtypes"})
class FlowEndpointControllerTest {

    @Mock private FlowTypeRegistry registry;
    @Mock private FlowOrchestrator orchestrator;
    @Mock private OrchestratorFlowRepository repository;

    private ObjectMapper objectMapper;
    private FlowEndpointController controller;
    private FlowTypeDescriptor descriptor;

    private static final String FLOW_TYPE = "testFlow";
    private static final String FLOW_ID = "flow-123";

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "controller_test_flows")
    static class TestFlow extends AbstractFlow {
        private String customField;
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        controller = new FlowEndpointController();

        ReflectionTestUtils.setField(controller, "registry", registry);
        ReflectionTestUtils.setField(controller, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(controller, "validator", null);
        ReflectionTestUtils.setField(controller, "searchApiEnabled", false);

        descriptor = FlowTypeDescriptor.builder()
                .flowType(FLOW_TYPE)
                .entityClass(TestFlow.class)
                .commandTopic("test-commands")
                .replyTopic("test-replies")
                .replyEnabled(false)
                .build();
        descriptor.setOrchestrator(orchestrator);
        descriptor.setRepository(repository);

        when(registry.resolve(FLOW_TYPE)).thenReturn(descriptor);
    }

    private TestFlow createTestFlow(FlowStatus status) {
        TestFlow flow = new TestFlow();
        ReflectionTestUtils.setField(flow, "id", FLOW_ID);
        flow.setCorrelationId("corr-" + FLOW_ID);
        flow.setStatus(status);
        flow.setCurrentStep("STEP_A");
        flow.setCustomField("test-value");
        return flow;
    }

    // ========== POST /flows/{flowType} — Start Flow ==========

    @Nested
    @DisplayName("POST /flows/{flowType} — startFlowByType")
    class StartFlowByType {

        @Test
        @DisplayName("starts flow successfully and returns 202 ACCEPTED")
        void startFlow_success() {
            TestFlow started = createTestFlow(FlowStatus.IN_PROGRESS);
            when(orchestrator.startFlow(any())).thenReturn(started);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("customField", "hello");

            ResponseEntity<?> response = controller.startFlowByType(FLOW_TYPE, body);

            assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("id"));
            assertEquals(FLOW_TYPE, result.get("flowType"));
            assertEquals(FlowStatus.IN_PROGRESS.name(), result.get("status"));
            assertEquals("STEP_A", result.get("currentStep"));
        }

        @Test
        @DisplayName("generates correlationId if missing from body")
        void startFlow_generatesCorrelationId() {
            TestFlow started = createTestFlow(FlowStatus.IN_PROGRESS);
            when(orchestrator.startFlow(any())).thenReturn(started);

            Map<String, Object> body = new LinkedHashMap<>();

            controller.startFlowByType(FLOW_TYPE, body);

            assertNotNull(body.get("correlationId"), "correlationId should be auto-generated");
        }

        @Test
        @DisplayName("preserves existing correlationId from body")
        void startFlow_preservesCorrelationId() {
            TestFlow started = createTestFlow(FlowStatus.IN_PROGRESS);
            when(orchestrator.startFlow(any())).thenReturn(started);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("correlationId", "my-custom-id");

            controller.startFlowByType(FLOW_TYPE, body);

            assertEquals("my-custom-id", body.get("correlationId"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void startFlow_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type 'unknown'"));

            Map<String, Object> body = new LinkedHashMap<>();
            ResponseEntity<?> response = controller.startFlowByType("unknown", body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertTrue(result.get("error").toString().contains("Unknown flow type"));
        }

        @Test
        @DisplayName("returns 400 when orchestrator throws exception")
        void startFlow_orchestratorError() {
            when(orchestrator.startFlow(any()))
                    .thenThrow(new IllegalArgumentException("Invalid flow entity: field required"));

            Map<String, Object> body = new LinkedHashMap<>();
            ResponseEntity<?> response = controller.startFlowByType(FLOW_TYPE, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertTrue(result.get("error").toString().contains("Invalid flow entity"));
        }
    }

    // ========== POST /flows — Single-flow backward compat ==========

    @Nested
    @DisplayName("POST /flows — startFlow (single-flow backward compat)")
    class StartFlowSingleType {

        @Test
        @DisplayName("starts single flow successfully")
        void startFlow_singleType_success() {
            when(registry.getSingleOrThrow()).thenReturn(descriptor);
            TestFlow started = createTestFlow(FlowStatus.IN_PROGRESS);
            when(orchestrator.startFlow(any())).thenReturn(started);

            Map<String, Object> body = new LinkedHashMap<>();
            ResponseEntity<?> response = controller.startFlow(body);

            assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 400 when multiple flow types registered")
        void startFlow_multipleTypes_error() {
            when(registry.getSingleOrThrow())
                    .thenThrow(new IllegalStateException("Multiple flow types"));
            when(registry.getFlowTypeNames()).thenReturn(Set.of("flowA", "flowB"));

            Map<String, Object> body = new LinkedHashMap<>();
            ResponseEntity<?> response = controller.startFlow(body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertTrue(result.get("error").toString().contains("Multiple flow types"));
        }
    }

    // ========== GET /flows/{flowType}/{id} ==========

    @Nested
    @DisplayName("GET /flows/{flowType}/{id} — getFlowByType")
    class GetFlowByType {

        @Test
        @DisplayName("returns flow when found")
        void getFlow_found() {
            TestFlow flow = createTestFlow(FlowStatus.COMPLETED);
            when(repository.findById(FLOW_ID)).thenReturn(Optional.of(flow));

            ResponseEntity<?> response = controller.getFlowByType(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertSame(flow, response.getBody());
        }

        @Test
        @DisplayName("returns 404 when flow not found")
        void getFlow_notFound() {
            when(repository.findById(FLOW_ID)).thenReturn(Optional.empty());

            ResponseEntity<?> response = controller.getFlowByType(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        }

        @Test
        @DisplayName("falls back to single-flow lookup on unknown flow type")
        void getFlow_fallbackToSingleFlow() {
            when(registry.resolve("unknownType"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));
            when(registry.getSingleOrThrow()).thenReturn(descriptor);
            TestFlow flow = createTestFlow(FlowStatus.COMPLETED);
            when(repository.findById("unknownType")).thenReturn(Optional.of(flow));

            ResponseEntity<?> response = controller.getFlowByType("unknownType", "someId");

            // It interprets "unknownType" as an ID in single-flow mode
            assertEquals(HttpStatus.OK, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 404 on fallback when multiple flow types and not found")
        void getFlow_fallbackNotFound() {
            when(registry.resolve("unknownType"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));
            when(registry.getSingleOrThrow())
                    .thenThrow(new IllegalStateException("Multiple flow types"));

            ResponseEntity<?> response = controller.getFlowByType("unknownType", "someId");

            assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        }
    }

    // ========== GET /flows/{flowType}/{id}/status ==========

    @Nested
    @DisplayName("GET /flows/{flowType}/{id}/status — getStatus")
    class GetStatus {

        @Test
        @DisplayName("returns status map when flow found")
        void getStatus_found() {
            TestFlow flow = createTestFlow(FlowStatus.IN_PROGRESS);
            flow.setRetryCount(2);
            flow.setErrorMessage("some error");
            when(repository.findById(FLOW_ID)).thenReturn(Optional.of(flow));

            ResponseEntity<?> response = controller.getStatus(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("id"));
            assertEquals(FLOW_TYPE, result.get("flowType"));
            assertEquals(FlowStatus.IN_PROGRESS.name(), result.get("status"));
            assertEquals("STEP_A", result.get("currentStep"));
            assertEquals(2, result.get("retryCount"));
            assertEquals("some error", result.get("errorMessage"));
        }

        @Test
        @DisplayName("returns status with empty strings for null currentStep and errorMessage")
        void getStatus_nullFields() {
            TestFlow flow = createTestFlow(FlowStatus.PENDING);
            flow.setCurrentStep(null);
            flow.setErrorMessage(null);
            when(repository.findById(FLOW_ID)).thenReturn(Optional.of(flow));

            ResponseEntity<?> response = controller.getStatus(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertEquals("", result.get("currentStep"));
            assertEquals("", result.get("errorMessage"));
        }

        @Test
        @DisplayName("returns 404 when flow not found")
        void getStatus_notFound() {
            when(repository.findById(FLOW_ID)).thenReturn(Optional.empty());

            ResponseEntity<?> response = controller.getStatus(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 404 for unknown flow type")
        void getStatus_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            ResponseEntity<?> response = controller.getStatus("unknown", FLOW_ID);

            assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        }
    }

    // ========== POST /flows/{flowType}/{id}/cancel ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/{id}/cancel — cancelFlow")
    class CancelFlow {

        @Test
        @DisplayName("cancels flow successfully")
        void cancel_success() {
            TestFlow cancelled = createTestFlow(FlowStatus.CANCELLED);
            cancelled.setErrorMessage("CANCELLED: user requested");
            when(orchestrator.cancelFlow(FLOW_ID, "too slow")).thenReturn(cancelled);

            Map<String, Object> body = Map.of("reason", "too slow");
            ResponseEntity<?> response = controller.cancelFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("flowId"));
            assertEquals(FlowStatus.CANCELLED.name(), result.get("status"));
        }

        @Test
        @DisplayName("uses default reason when body is null")
        void cancel_nullBody() {
            TestFlow cancelled = createTestFlow(FlowStatus.CANCELLED);
            cancelled.setErrorMessage("CANCELLED: user requested");
            when(orchestrator.cancelFlow(FLOW_ID, "user requested")).thenReturn(cancelled);

            ResponseEntity<?> response = controller.cancelFlow(FLOW_TYPE, FLOW_ID, null);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            verify(orchestrator).cancelFlow(FLOW_ID, "user requested");
        }

        @Test
        @DisplayName("uses default reason when body has no reason key")
        void cancel_noReasonInBody() {
            TestFlow cancelled = createTestFlow(FlowStatus.CANCELLED);
            cancelled.setErrorMessage("CANCELLED: user requested");
            when(orchestrator.cancelFlow(FLOW_ID, "user requested")).thenReturn(cancelled);

            Map<String, Object> body = new HashMap<>();
            ResponseEntity<?> response = controller.cancelFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            verify(orchestrator).cancelFlow(FLOW_ID, "user requested");
        }

        @Test
        @DisplayName("returns 400 when flow not in cancellable state")
        void cancel_notCancellable() {
            when(orchestrator.cancelFlow(FLOW_ID, "user requested")).thenReturn(null);

            ResponseEntity<?> response = controller.cancelFlow(FLOW_TYPE, FLOW_ID, null);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertTrue(result.get("error").toString().contains("Cannot cancel flow"));
            assertEquals(FLOW_ID, result.get("flowId"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void cancel_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type 'unknown'"));

            ResponseEntity<?> response = controller.cancelFlow("unknown", FLOW_ID, null);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }

        @Test
        @DisplayName("cancelled flow with null currentStep returns empty string")
        void cancel_nullCurrentStep() {
            TestFlow cancelled = createTestFlow(FlowStatus.CANCELLED);
            cancelled.setCurrentStep(null);
            cancelled.setErrorMessage("CANCELLED: cleanup");
            when(orchestrator.cancelFlow(FLOW_ID, "cleanup")).thenReturn(cancelled);

            Map<String, Object> body = Map.of("reason", "cleanup");
            ResponseEntity<?> response = controller.cancelFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertEquals("", result.get("currentStep"));
        }
    }

    // ========== POST /flows/{flowType}/{id}/signal ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/{id}/signal — signalFlow")
    class SignalFlow {

        @Test
        @DisplayName("sends signal successfully")
        void signal_success() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("signalName", "approve");
            body.put("payload", Map.of("approver", "admin"));

            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("flowId"));
            assertEquals("approve", result.get("signal"));
            assertEquals("Signal delivered", result.get("message"));

            verify(orchestrator).signal(eq(FLOW_ID), eq("approve"), eq(Map.of("approver", "admin")));
        }

        @Test
        @DisplayName("uses empty map when payload is missing")
        void signal_noPayload() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("signalName", "wake");

            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            verify(orchestrator).signal(eq(FLOW_ID), eq("wake"), eq(Map.of()));
        }

        @Test
        @DisplayName("returns 400 when signalName is null")
        void signal_nullSignalName() {
            Map<String, Object> body = new LinkedHashMap<>();

            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("signalName is required"));
        }

        @Test
        @DisplayName("returns 400 when signalName is blank")
        void signal_blankSignalName() {
            Map<String, Object> body = Map.of("signalName", "   ");

            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("signalName is required"));
        }

        @Test
        @DisplayName("returns 400 for unknown signal")
        void signal_unknownSignal() {
            doThrow(new IllegalArgumentException("Unknown signal 'bogus'"))
                    .when(orchestrator).signal(eq(FLOW_ID), eq("bogus"), any());

            Map<String, Object> body = Map.of("signalName", "bogus");
            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("Unknown signal"));
        }

        @Test
        @DisplayName("returns 400 for IllegalStateException (no signals registered)")
        void signal_noSignalsRegistered() {
            doThrow(new IllegalStateException("No signals registered"))
                    .when(orchestrator).signal(eq(FLOW_ID), eq("approve"), any());

            Map<String, Object> body = Map.of("signalName", "approve");
            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 500 for unexpected RuntimeException")
        void signal_unexpectedError() {
            doThrow(new RuntimeException("Kafka down"))
                    .when(orchestrator).signal(eq(FLOW_ID), eq("approve"), any());

            Map<String, Object> body = Map.of("signalName", "approve");
            ResponseEntity<?> response = controller.signalFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("Kafka down"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void signal_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            Map<String, Object> body = Map.of("signalName", "approve");
            ResponseEntity<?> response = controller.signalFlow("unknown", FLOW_ID, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }

    // ========== POST /flows/{flowType}/{id}/retry-compensation ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/{id}/retry-compensation — retryCompensation")
    class RetryCompensation {

        @Test
        @DisplayName("retries compensation for COMPENSATION_FAILED flow")
        void retryCompensation_success() {
            TestFlow flow = createTestFlow(FlowStatus.COMPENSATION_FAILED);
            TestFlow updatedFlow = createTestFlow(FlowStatus.FAILED);
            when(repository.findById(FLOW_ID))
                    .thenReturn(Optional.of(flow))
                    .thenReturn(Optional.of(updatedFlow));

            ResponseEntity<?> response = controller.retryCompensation(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("flowId"));
            assertEquals(FlowStatus.FAILED.name(), result.get("status"));
            assertEquals("Compensation retried", result.get("message"));
            verify(orchestrator).retryCompensation(FLOW_ID);
        }

        @Test
        @DisplayName("returns 404 when flow not found")
        void retryCompensation_notFound() {
            when(repository.findById(FLOW_ID)).thenReturn(Optional.empty());

            ResponseEntity<?> response = controller.retryCompensation(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 400 when flow is not in COMPENSATION_FAILED status")
        void retryCompensation_wrongStatus() {
            TestFlow flow = createTestFlow(FlowStatus.IN_PROGRESS);
            when(repository.findById(FLOW_ID)).thenReturn(Optional.of(flow));

            ResponseEntity<?> response = controller.retryCompensation(FLOW_TYPE, FLOW_ID);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("not in COMPENSATION_FAILED"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void retryCompensation_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            ResponseEntity<?> response = controller.retryCompensation("unknown", FLOW_ID);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }

    // ========== POST /flows/{flowType}/{id}/replay ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/{id}/replay — replayFlow")
    class ReplayFlow {

        @Test
        @DisplayName("replays flow successfully with no body")
        void replay_success_noBody() {
            TestFlow replayed = createTestFlow(FlowStatus.IN_PROGRESS);
            when(orchestrator.replayFlow(eq(FLOW_ID), any(ReplayOptions.class))).thenReturn(replayed);

            ResponseEntity<?> response = controller.replayFlow(FLOW_TYPE, FLOW_ID, null);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_ID, result.get("flowId"));
            assertEquals(FlowStatus.IN_PROGRESS.name(), result.get("status"));
            assertEquals("STEP_A", result.get("currentStep"));
            assertEquals("Flow replayed", result.get("message"));
        }

        @Test
        @DisplayName("replays flow with fromStep and allowCompleted options")
        void replay_withOptions() {
            TestFlow replayed = createTestFlow(FlowStatus.IN_PROGRESS);
            replayed.setCurrentStep("STEP_B");

            ArgumentCaptor<ReplayOptions> optionsCaptor = ArgumentCaptor.forClass(ReplayOptions.class);
            when(orchestrator.replayFlow(eq(FLOW_ID), optionsCaptor.capture())).thenReturn(replayed);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("fromStep", "STEP_B");
            body.put("allowCompleted", true);

            ResponseEntity<?> response = controller.replayFlow(FLOW_TYPE, FLOW_ID, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            ReplayOptions captured = optionsCaptor.getValue();
            assertEquals("STEP_B", captured.getFromStep());
            assertTrue(captured.isAllowCompleted());
        }

        @Test
        @DisplayName("returns 400 when replay not allowed (flow in progress)")
        void replay_notAllowed() {
            when(orchestrator.replayFlow(eq(FLOW_ID), any(ReplayOptions.class)))
                    .thenThrow(new IllegalStateException("Cannot replay flow — status is IN_PROGRESS"));

            ResponseEntity<?> response = controller.replayFlow(FLOW_TYPE, FLOW_ID, null);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("Cannot replay"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void replay_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            ResponseEntity<?> response = controller.replayFlow("unknown", FLOW_ID, null);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 400 when flow not found")
        void replay_flowNotFound() {
            when(orchestrator.replayFlow(eq(FLOW_ID), any(ReplayOptions.class)))
                    .thenThrow(new IllegalArgumentException("Flow not found: " + FLOW_ID));

            ResponseEntity<?> response = controller.replayFlow(FLOW_TYPE, FLOW_ID, null);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }

    // ========== POST /flows/{flowType}/ops/batch-replay ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/ops/batch-replay — replayFlows")
    class BatchReplay {

        @Test
        @DisplayName("batch replays with all success")
        void batchReplay_allSuccess() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "replayed"),
                    Map.of("flowId", "id2", "status", "replayed"));
            when(orchestrator.replayFlows(anyList(), any(ReplayOptions.class))).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1", "id2"));

            ResponseEntity<?> response = controller.replayFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(2, result.get("total"));
            assertEquals(2L, result.get("succeeded"));
            assertEquals(0L, result.get("failed"));
        }

        @Test
        @DisplayName("batch replays with mixed success and failure")
        void batchReplay_mixed() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "replayed"),
                    Map.of("flowId", "id2", "status", "error", "error", "Flow not found"));
            when(orchestrator.replayFlows(anyList(), any(ReplayOptions.class))).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1", "id2"));

            ResponseEntity<?> response = controller.replayFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(2, result.get("total"));
            assertEquals(1L, result.get("succeeded"));
            assertEquals(1L, result.get("failed"));
        }

        @Test
        @DisplayName("returns 400 when flowIds is null")
        void batchReplay_nullFlowIds() {
            Map<String, Object> body = new LinkedHashMap<>();

            ResponseEntity<?> response = controller.replayFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("flowIds is required"));
        }

        @Test
        @DisplayName("returns 400 when flowIds is empty")
        void batchReplay_emptyFlowIds() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of());

            ResponseEntity<?> response = controller.replayFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("flowIds is required"));
        }

        @Test
        @DisplayName("passes fromStep and allowCompleted options to orchestrator")
        void batchReplay_withOptions() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "replayed"));
            ArgumentCaptor<ReplayOptions> optionsCaptor = ArgumentCaptor.forClass(ReplayOptions.class);
            when(orchestrator.replayFlows(anyList(), optionsCaptor.capture())).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1"));
            body.put("fromStep", "STEP_B");
            body.put("allowCompleted", true);

            controller.replayFlows(FLOW_TYPE, body);

            ReplayOptions captured = optionsCaptor.getValue();
            assertEquals("STEP_B", captured.getFromStep());
            assertTrue(captured.isAllowCompleted());
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void batchReplay_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            Map<String, Object> body = Map.of("flowIds", List.of("id1"));
            ResponseEntity<?> response = controller.replayFlows("unknown", body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }

    // ========== POST /flows/{flowType}/ops/batch-cancel ==========

    @Nested
    @DisplayName("POST /flows/{flowType}/ops/batch-cancel — cancelFlows")
    class BatchCancel {

        @Test
        @DisplayName("batch cancels with all success")
        void batchCancel_allSuccess() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "cancelled"),
                    Map.of("flowId", "id2", "status", "cancelled"));
            when(orchestrator.cancelFlows(anyList(), eq("bulk cleanup"))).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1", "id2"));
            body.put("reason", "bulk cleanup");

            ResponseEntity<?> response = controller.cancelFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(2, result.get("total"));
            assertEquals(2L, result.get("succeeded"));
            assertEquals(0L, result.get("failed"));
        }

        @Test
        @DisplayName("batch cancels with mixed success and failure")
        void batchCancel_mixed() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "cancelled"),
                    Map.of("flowId", "id2", "status", "error", "error", "Not cancellable"));
            when(orchestrator.cancelFlows(anyList(), eq("batch cancel"))).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1", "id2"));

            ResponseEntity<?> response = controller.cancelFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertEquals(2, result.get("total"));
            assertEquals(1L, result.get("succeeded"));
            assertEquals(1L, result.get("failed"));
        }

        @Test
        @DisplayName("uses default reason when not provided")
        void batchCancel_defaultReason() {
            List<Map<String, String>> results = List.of(
                    Map.of("flowId", "id1", "status", "cancelled"));
            when(orchestrator.cancelFlows(anyList(), eq("batch cancel"))).thenReturn(results);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of("id1"));

            controller.cancelFlows(FLOW_TYPE, body);

            verify(orchestrator).cancelFlows(anyList(), eq("batch cancel"));
        }

        @Test
        @DisplayName("returns 400 when flowIds is null")
        void batchCancel_nullFlowIds() {
            Map<String, Object> body = new LinkedHashMap<>();

            ResponseEntity<?> response = controller.cancelFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("flowIds is required"));
        }

        @Test
        @DisplayName("returns 400 when flowIds is empty")
        void batchCancel_emptyFlowIds() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("flowIds", List.of());

            ResponseEntity<?> response = controller.cancelFlows(FLOW_TYPE, body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }

        @Test
        @DisplayName("returns 400 for unknown flow type")
        void batchCancel_unknownFlowType() {
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            Map<String, Object> body = Map.of("flowIds", List.of("id1"));
            ResponseEntity<?> response = controller.cancelFlows("unknown", body);

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }

    // ========== GET /flows/{flowType}/search ==========

    @Nested
    @DisplayName("GET /flows/{flowType}/search — searchFlows")
    class SearchFlows {

        @Test
        @DisplayName("returns 403 when search API is disabled")
        void search_disabled() {
            Map<String, String> params = Map.of("customField", "value");

            ResponseEntity<?> response = controller.searchFlows(FLOW_TYPE, params);

            assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertTrue(result.get("error").toString().contains("Search API is disabled"));
        }

        @Test
        @DisplayName("returns search results when enabled")
        void search_success() {
            ReflectionTestUtils.setField(controller, "searchApiEnabled", true);
            TestFlow flow = createTestFlow(FlowStatus.COMPLETED);
            when(orchestrator.findFlows(anyMap())).thenReturn(List.of(flow));

            Map<String, String> params = Map.of("customField", "test-value");

            ResponseEntity<?> response = controller.searchFlows(FLOW_TYPE, params);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            Map<String, Object> result = (Map<String, Object>) response.getBody();
            assertNotNull(result);
            assertEquals(FLOW_TYPE, result.get("flowType"));
            assertEquals(1, result.get("count"));
            assertNotNull(result.get("flows"));
        }

        @Test
        @DisplayName("returns 400 for unknown flow type when search enabled")
        void search_unknownFlowType() {
            ReflectionTestUtils.setField(controller, "searchApiEnabled", true);
            when(registry.resolve("unknown"))
                    .thenThrow(new IllegalArgumentException("Unknown flow type"));

            ResponseEntity<?> response = controller.searchFlows("unknown", Map.of());

            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
    }
}
