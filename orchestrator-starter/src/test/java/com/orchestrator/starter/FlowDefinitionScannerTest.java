package com.orchestrator.starter;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowDefinition;
import com.orchestrator.starter.flow.FlowDefinitionScanner;
import com.orchestrator.starter.flow.StepHandler;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FlowDefinitionScannerTest {

    static class TestFlow implements OrchestratorFlow {
        public String getId() { return "t"; }
        public String getCorrelationId() { return "c"; }
        public String getCurrentStep() { return "S"; }
        public void setCurrentStep(String s) {}
        public FlowStatus getStatus() { return FlowStatus.PENDING; }
        public void setStatus(FlowStatus s) {}
        public int getRetryCount() { return 0; }
        public void setRetryCount(int c) {}
        public int getBackoffSeconds() { return 0; }
        public void setBackoffSeconds(int s) {}
        public Instant getNextRetryAt() { return null; }
        public void setNextRetryAt(Instant i) {}
        public String getErrorMessage() { return null; }
        public void setErrorMessage(String m) {}
        public Instant getUpdatedAt() { return Instant.now(); }
        public void setUpdatedAt(Instant i) {}
    }

    // Valid flow
    @Flow
    static class ValidFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void stepA(TestFlow flow) {}

        @Step(order = 2)
        public void stepB(TestFlow flow) {}

        @Compensate(step = "stepA")
        public void undoA(TestFlow flow) {}
    }

    // Invalid: @Compensate references non-existent step
    @Flow
    static class InvalidCompensateFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void stepA(TestFlow flow) {}

        @Compensate(step = "nonExistent")
        public void undoX(TestFlow flow) {}
    }

    // Invalid: duplicate order
    @Flow
    static class DuplicateOrderFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void stepA(TestFlow flow) {}

        @Step(order = 1)
        public void stepB(TestFlow flow) {}
    }

    // Invalid: @JoinOn references non-existent parallel group
    @Flow
    static class InvalidJoinFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void stepA(TestFlow flow) {}

        @Step(order = 2)
        @JoinOn(group = "nonExistent")
        public void joinStep(TestFlow flow) {}
    }

    @Configuration
    static class ValidConfig {
        @Bean ValidFlow validFlow() { return new ValidFlow(); }
    }

    @Configuration
    static class InvalidCompensateConfig {
        @Bean InvalidCompensateFlow flow() { return new InvalidCompensateFlow(); }
    }

    @Configuration
    static class DuplicateOrderConfig {
        @Bean DuplicateOrderFlow flow() { return new DuplicateOrderFlow(); }
    }

    @Configuration
    static class InvalidJoinConfig {
        @Bean InvalidJoinFlow flow() { return new InvalidJoinFlow(); }
    }

    @Test
    void discoversValidFlow() {
        var ctx = new AnnotationConfigApplicationContext(ValidConfig.class);
        List<StepHandler> steps = FlowDefinitionScanner.scan(ctx);
        assertEquals(2, steps.size());
        assertEquals("STEP_A", steps.get(0).getStepName());
        assertEquals("STEP_B", steps.get(1).getStepName());
        ctx.close();
    }

    @Test
    void failsOnInvalidCompensateReference() {
        var ctx = new AnnotationConfigApplicationContext(InvalidCompensateConfig.class);
        assertThrows(IllegalStateException.class, () -> FlowDefinitionScanner.scan(ctx));
        ctx.close();
    }

    @Test
    void failsOnDuplicateOrder() {
        var ctx = new AnnotationConfigApplicationContext(DuplicateOrderConfig.class);
        assertThrows(IllegalStateException.class, () -> FlowDefinitionScanner.scan(ctx));
        ctx.close();
    }

    @Test
    void failsOnInvalidJoinGroup() {
        var ctx = new AnnotationConfigApplicationContext(InvalidJoinConfig.class);
        assertThrows(IllegalStateException.class, () -> FlowDefinitionScanner.scan(ctx));
        ctx.close();
    }

}
