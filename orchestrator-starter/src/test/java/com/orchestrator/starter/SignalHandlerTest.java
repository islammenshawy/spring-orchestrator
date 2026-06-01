package com.orchestrator.starter;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.SignalHandler;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class SignalHandlerTest {

    // ========== Test flow ==========

    static class TestFlow implements OrchestratorFlow {
        private String id = "flow-1";
        private String correlationId = "corr-1";
        private String currentStep = "STEP_A";
        private FlowStatus status = FlowStatus.PENDING;
        private int retryCount;
        private int backoffSeconds;
        private Instant nextRetryAt;
        private String errorMessage;
        private Instant updatedAt = Instant.now();
        private String signalValue;

        public String getId() { return id; }
        public String getCorrelationId() { return correlationId; }
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
        public String getSignalValue() { return signalValue; }
        public void setSignalValue(String v) { this.signalValue = v; }
    }

    // ========== Flow definition with signal methods ==========

    static class TestFlowDefinition {
        /** Signal with no payload (1 parameter). */
        public void onApprove(TestFlow flow) {
            flow.setSignalValue("approved");
        }

        /** Signal with payload (2 parameters). */
        public void onUpdate(TestFlow flow, String payload) {
            flow.setSignalValue("updated:" + payload);
        }

        /** Signal that throws a RuntimeException. */
        public void onFailRuntime(TestFlow flow) {
            throw new IllegalStateException("runtime-fail");
        }

        /** Signal that throws a checked exception wrapper. */
        public void onFailChecked(TestFlow flow) throws Exception {
            throw new Exception("checked-fail");
        }
    }

    // ========== Tests ==========

    @Test
    void getSignalName_returnsConstructorValue() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onApprove", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "APPROVE");
        assertEquals("APPROVE", handler.getSignalName());
    }

    @Test
    void getPayloadType_nullWhenSingleParameter() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onApprove", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "APPROVE");
        assertNull(handler.getPayloadType());
    }

    @Test
    void getPayloadType_returnsSecondParameterType() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onUpdate", TestFlow.class, String.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "UPDATE");
        assertEquals(String.class, handler.getPayloadType());
    }

    @Test
    void invoke_noPayload_executesHandler() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onApprove", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "APPROVE");

        var flow = new TestFlow();
        handler.invoke(flow, null);

        assertEquals("approved", flow.getSignalValue());
    }

    @Test
    void invoke_withPayload_executesHandlerWithPayload() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onUpdate", TestFlow.class, String.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "UPDATE");

        var flow = new TestFlow();
        handler.invoke(flow, "new-data");

        assertEquals("updated:new-data", flow.getSignalValue());
    }

    @Test
    void invoke_withPayloadMethod_nullPayload_invokesWithoutPayload() throws Exception {
        // When payloadType is non-null but payload is null, invoke with just the flow
        // This calls the 1-arg overload which will fail because the method expects 2 params.
        // Actually checking the code: if payloadType != null && payload != null -> 2 args, else 1 arg.
        // So for a 2-param method called with null payload, it calls method.invoke(def, flow) which throws.
        Method method = TestFlowDefinition.class.getMethod("onUpdate", TestFlow.class, String.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "UPDATE");

        var flow = new TestFlow();
        // The method expects 2 params but gets 1 — reflection throws IllegalArgumentException
        // which is wrapped as RuntimeException by the handler
        assertThrows(RuntimeException.class, () -> handler.invoke(flow, null));
    }

    @Test
    void invoke_runtimeException_rethrown() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onFailRuntime", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "FAIL_RUNTIME");

        var flow = new TestFlow();
        var thrown = assertThrows(IllegalStateException.class, () -> handler.invoke(flow, null));
        assertEquals("runtime-fail", thrown.getMessage());
    }

    @Test
    void invoke_checkedException_wrappedInRuntimeException() throws Exception {
        Method method = TestFlowDefinition.class.getMethod("onFailChecked", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "FAIL_CHECKED");

        var flow = new TestFlow();
        var thrown = assertThrows(RuntimeException.class, () -> handler.invoke(flow, null));
        assertTrue(thrown.getMessage().contains("Signal handler failed: FAIL_CHECKED"));
        assertNotNull(thrown.getCause());
        assertEquals("checked-fail", thrown.getCause().getMessage());
    }

    @Test
    void invoke_noPayloadMethod_payloadIgnored() throws Exception {
        // When method has 1 param but payload is provided, it should still invoke with just flow
        // because payloadType is null (1-param method)
        Method method = TestFlowDefinition.class.getMethod("onApprove", TestFlow.class);
        var handler = new SignalHandler<TestFlow>(new TestFlowDefinition(), method, "APPROVE");

        var flow = new TestFlow();
        handler.invoke(flow, "ignored-payload");

        assertEquals("approved", flow.getSignalValue());
    }
}
