package com.orchestrator.starter;

import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.idempotency.IdempotencyService;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaConsumerTest {

    private FlowOrchestrator orchestrator;
    private IdempotencyService idempotencyService;
    private OrchestratorKafkaConsumer consumer;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        orchestrator = mock(FlowOrchestrator.class);
        idempotencyService = mock(IdempotencyService.class);
        FlowTypeDescriptor descriptor = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(Object.class).commandTopic("commands")
                .replyTopic("commands.replies").replyEnabled(true)
                .orchestrator(orchestrator).build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(descriptor));
        consumer = new OrchestratorKafkaConsumer(registry, idempotencyService, objectMapper, true);
    }

    private String commandJson(String flowId, String step, String eventId) {
        return "{\"flowId\":\"" + flowId + "\",\"stepName\":\"" + step + "\",\"eventId\":\"" + eventId + "\"}";
    }

    // ========== Layer 1: check before, mark after ==========

    @Test
    void onStepCommand_newEvent_executesAndMarksAfter() {
        when(idempotencyService.isProcessed("evt-1")).thenReturn(false);

        consumer.onStepCommand(commandJson("flow-1", "STEP_A", "evt-1"), "commands", 0);

        // Verify order: check → execute → mark
        var inOrder = inOrder(idempotencyService, orchestrator);
        inOrder.verify(idempotencyService).isProcessed("evt-1");
        inOrder.verify(orchestrator).executeStepOnly("flow-1", "STEP_A");
        inOrder.verify(idempotencyService).tryProcess("evt-1");
    }

    @Test
    void onStepCommand_alreadyProcessed_skipsEverything() {
        when(idempotencyService.isProcessed("evt-1")).thenReturn(true);

        consumer.onStepCommand(commandJson("flow-1", "STEP_A", "evt-1"), "commands", 0);

        verify(orchestrator, never()).executeStepOnly(any(), any());
        verify(orchestrator, never()).executeStep(any(), any());
        verify(idempotencyService, never()).tryProcess(any());
    }

    @Test
    void onStepCommand_stepFails_doesNotMarkProcessed() {
        when(idempotencyService.isProcessed("evt-1")).thenReturn(false);
        doThrow(new RetryableStepException("timeout")).when(orchestrator).executeStepOnly(any(), any());

        assertThrows(RetryableStepException.class, () ->
                consumer.onStepCommand(commandJson("flow-1", "STEP_A", "evt-1"), "commands", 0));

        // NOT marked as processed — retry will re-enter
        verify(idempotencyService, never()).tryProcess(any());
    }

    @Test
    void onStepCommand_stepFailsWithInfraError_doesNotMarkProcessed() {
        when(idempotencyService.isProcessed("evt-1")).thenReturn(false);
        doThrow(new RetryableStepException("Infrastructure error: MongoDB down"))
                .when(orchestrator).executeStepOnly(any(), any());

        assertThrows(RetryableStepException.class, () ->
                consumer.onStepCommand(commandJson("flow-1", "STEP_A", "evt-1"), "commands", 0));

        verify(idempotencyService, never()).tryProcess(any());
    }

    // ========== Reply consumer ==========

    @Test
    void onStepReply_completed_advancesFlow() {
        when(idempotencyService.isProcessed("reply-1")).thenReturn(false);
        String replyJson = "{\"flowId\":\"flow-1\",\"stepName\":\"STEP_A\",\"eventId\":\"reply-1\",\"status\":\"COMPLETED\"}";

        consumer.onStepReply(replyJson, "commands.replies", 0);

        verify(orchestrator).advanceAfterReply(eq("flow-1"), eq("STEP_A"), any());
        verify(idempotencyService).tryProcess("reply-1");
    }

    @Test
    void onStepReply_failed_doesNotAdvance() {
        when(idempotencyService.isProcessed("reply-1")).thenReturn(false);
        String replyJson = "{\"flowId\":\"flow-1\",\"stepName\":\"STEP_A\",\"eventId\":\"reply-1\",\"status\":\"FAILED\",\"errorMessage\":\"bad request\"}";

        consumer.onStepReply(replyJson, "commands.replies", 0);

        verify(orchestrator, never()).advanceAfterReply(any(), any(), any());
        verify(idempotencyService).tryProcess("reply-1");
    }

    @Test
    void onStepReply_alreadyProcessed_skips() {
        when(idempotencyService.isProcessed("reply-1")).thenReturn(true);
        String replyJson = "{\"flowId\":\"flow-1\",\"stepName\":\"STEP_A\",\"eventId\":\"reply-1\",\"status\":\"COMPLETED\"}";

        consumer.onStepReply(replyJson, "commands.replies", 0);

        verify(orchestrator, never()).advanceAfterReply(any(), any(), any());
    }

    // ========== DLT ==========

    @Test
    void onDlt_capturesExceptionMessage() {
        String cmdJson = commandJson("flow-1", "STEP_A", "evt-1");

        consumer.onDlt(cmdJson, "commands-dlt", 0, "HTTP 500 on vendor API after 3 retries");

        verify(orchestrator).markDeadLettered("flow-1", "STEP_A", "HTTP 500 on vendor API after 3 retries");
        verify(idempotencyService).tryProcess("evt-1");
    }

    @Test
    void onDlt_nullException_passesUnknown() {
        String cmdJson = commandJson("flow-1", "STEP_A", "evt-1");

        consumer.onDlt(cmdJson, "commands-dlt", 0);

        verify(orchestrator).markDeadLettered("flow-1", "STEP_A", "unknown");
    }

    // ========== Deserialization ==========

    @Test
    void onStepCommand_invalidJson_throwsRuntime() {
        assertThrows(RuntimeException.class, () ->
                consumer.onStepCommand("not json", "commands", 0));
    }

    // ========== Inline mode ==========

    @Test
    void onStepCommand_inlineMode_callsExecuteStepNotExecuteStepOnly() {
        FlowTypeDescriptor inlineDesc = FlowTypeDescriptor.builder()
                .flowType("test").entityClass(Object.class).commandTopic("commands")
                .replyTopic("").replyEnabled(false)
                .orchestrator(orchestrator).build();
        FlowTypeRegistry inlineRegistry = new FlowTypeRegistry(List.of(inlineDesc));
        var inlineConsumer = new OrchestratorKafkaConsumer(inlineRegistry, idempotencyService, objectMapper, false);
        when(idempotencyService.isProcessed("evt-1")).thenReturn(false);

        inlineConsumer.onStepCommand(commandJson("flow-1", "STEP_A", "evt-1"), "commands", 0);

        verify(orchestrator).executeStep("flow-1", "STEP_A");
        verify(orchestrator, never()).executeStepOnly(any(), any());
    }
}
