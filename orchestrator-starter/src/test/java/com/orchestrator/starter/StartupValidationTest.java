package com.orchestrator.starter;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowDefinition;
import com.orchestrator.starter.flow.FlowDefinitionScanner;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that prove: "Any @SpringBootTest will catch misconfiguration."
 *
 * These tests simulate what happens when a developer makes a mistake
 * in their @Flow/@Step/@Compensate/@Parallel/@JoinOn annotations.
 * The Spring context fails to start with a clear error message.
 */
class StartupValidationTest {

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

    // ======== Invalid: @Compensate references wrong method ========

    @Flow
    static class BadCompensateFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void doWork(TestFlow flow) {}

        @Compensate(step = "typoMethodName")  // ← doesn't exist
        public void undoWork(TestFlow flow) {}
    }

    @Configuration
    static class BadCompensateConfig {
        @Bean BadCompensateFlow flow() { return new BadCompensateFlow(); }
    }

    @Test
    void failsStartupOnBadCompensateReference() {
        var ctx = new AnnotationConfigApplicationContext(BadCompensateConfig.class);
        var ex = assertThrows(IllegalStateException.class,
                () -> FlowDefinitionScanner.scan(ctx));
        assertTrue(ex.getMessage().contains("typoMethodName"),
                "Error should mention the bad reference: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("doWork"),
                "Error should list available steps: " + ex.getMessage());
        ctx.close();
    }

    // ======== Invalid: @JoinOn references non-existent group ========

    @Flow
    static class BadJoinFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void doWork(TestFlow flow) {}

        @Step(order = 2)
        @JoinOn(group = "nonExistentGroup")  // ← no @Parallel with this group
        public void joinStep(TestFlow flow) {}
    }

    @Configuration
    static class BadJoinConfig {
        @Bean BadJoinFlow flow() { return new BadJoinFlow(); }
    }

    @Test
    void failsStartupOnBadJoinGroupReference() {
        var ctx = new AnnotationConfigApplicationContext(BadJoinConfig.class);
        var ex = assertThrows(IllegalStateException.class,
                () -> FlowDefinitionScanner.scan(ctx));
        assertTrue(ex.getMessage().contains("nonExistentGroup"),
                "Error should mention the bad group: " + ex.getMessage());
        ctx.close();
    }

    // ======== Invalid: duplicate step order ========

    @Flow
    static class DuplicateOrderFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void stepA(TestFlow flow) {}

        @Step(order = 1)  // ← same order, not @Parallel
        public void stepB(TestFlow flow) {}
    }

    @Configuration
    static class DuplicateOrderConfig {
        @Bean DuplicateOrderFlow flow() { return new DuplicateOrderFlow(); }
    }

    @Test
    void failsStartupOnDuplicateStepOrder() {
        var ctx = new AnnotationConfigApplicationContext(DuplicateOrderConfig.class);
        assertThrows(IllegalStateException.class,
                () -> FlowDefinitionScanner.scan(ctx));
        ctx.close();
    }

    // ======== Invalid: wrong method signature ========

    @Flow
    static class BadSignatureFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void badStep(String notAFlow) {}  // ← wrong parameter type
    }

    @Configuration
    static class BadSignatureConfig {
        @Bean BadSignatureFlow flow() { return new BadSignatureFlow(); }
    }

    @Test
    void failsStartupOnWrongMethodSignature() {
        var ctx = new AnnotationConfigApplicationContext(BadSignatureConfig.class);
        assertThrows(IllegalStateException.class,
                () -> FlowDefinitionScanner.scan(ctx));
        ctx.close();
    }

    // ======== Valid: minimal flow (proves good config passes) ========

    @Flow
    static class MinimalValidFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void onlyStep(TestFlow flow) {}
    }

    @Configuration
    static class ValidConfig {
        @Bean MinimalValidFlow flow() { return new MinimalValidFlow(); }
    }

    @Test
    void validFlowPassesStartup() {
        var ctx = new AnnotationConfigApplicationContext(ValidConfig.class);
        var steps = FlowDefinitionScanner.scan(ctx);
        assertEquals(1, steps.size());
        assertEquals("ONLY_STEP", steps.get(0).getStepName());
        ctx.close();
    }

    // ======== Valid: flow with compensation (proves good config) ========

    @Flow
    static class ValidCompensateFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void doWork(TestFlow flow) {}

        @Compensate(step = "doWork")
        public void undoWork(TestFlow flow) {}

        @Step(order = 2)
        public void doMoreWork(TestFlow flow) {}
    }

    @Configuration
    static class ValidCompensateConfig {
        @Bean ValidCompensateFlow flow() { return new ValidCompensateFlow(); }
    }

    @Test
    void validCompensateFlowPassesStartup() {
        var ctx = new AnnotationConfigApplicationContext(ValidCompensateConfig.class);
        var steps = FlowDefinitionScanner.scan(ctx);
        assertEquals(2, steps.size());
        ctx.close();
    }
}
