package com.orchestrator.starter;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.flow.FlowDefinition;
import com.orchestrator.starter.flow.FlowDefinitionScanner;
import com.orchestrator.starter.flow.StepHandler;
import com.orchestrator.starter.flow.StepRegistry;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parallel step execution and join points.
 * Validates: step ordering, parallel grouping, join resolution,
 * and scanner validation for invalid configurations.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class ParallelJoinFlowTest {

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "test_flows")
    static class TestFlow extends AbstractFlow {
        private String result;
    }

    // ========== Valid: Sequential → Parallel → Join → Sequential ==========

    @Flow(name = "parallel-test")
    static class ParallelJoinFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void init(TestFlow flow) { flow.setResult("init"); }

        @Step(order = 2)
        @Parallel(group = "processing")
        public void processA(TestFlow flow) {}

        @Step(order = 2)
        @Parallel(group = "processing")
        public void processB(TestFlow flow) {}

        @Step(order = 3)
        @JoinOn(group = "processing")
        public void mergeResults(TestFlow flow) {}

        @Step(order = 4)
        public void finalize(TestFlow flow) {}
    }

    @Configuration
    static class ParallelJoinConfig {
        @Bean ParallelJoinFlow flow() { return new ParallelJoinFlow(); }
    }

    @Test
    void parallelJoin_scansCorrectly() {
        var ctx = new AnnotationConfigApplicationContext(ParallelJoinConfig.class);
        Map<String, FlowDefinitionScanner.FlowTypeInfo> flows =
                FlowDefinitionScanner.scanByFlowType(ctx);

        assertEquals(1, flows.size());
        var info = flows.get("parallel-test");
        assertNotNull(info);
        assertEquals(5, info.handlers().size()); // 5 steps total

        // Build registry and verify structure
        StepRegistry<TestFlow> registry = new StepRegistry<>(info.handlers());

        // Step 1: INIT (sequential)
        assertEquals("INIT", registry.getFirstStep());
        assertEquals("PROCESS_A", registry.getNextStep("INIT"));

        // Step 2: PROCESS_A and PROCESS_B (parallel, same order)
        List<String> parallel = registry.getStepsAtSameOrder("PROCESS_A");
        assertEquals(2, parallel.size());
        assertTrue(parallel.contains("PROCESS_A"));
        assertTrue(parallel.contains("PROCESS_B"));

        // Step 3: MERGE_RESULTS (join)
        assertEquals("MERGE_RESULTS", registry.getNextStep("PROCESS_A"));
        assertEquals("MERGE_RESULTS", registry.getNextStep("PROCESS_B"));

        // Step 4: FINALIZE (sequential)
        assertEquals("FINALIZE", registry.getNextStep("MERGE_RESULTS"));
        assertNull(registry.getNextStep("FINALIZE")); // last step

        ctx.close();
    }

    @Test
    void parallelGroup_returnsCorrectHandlers() {
        var ctx = new AnnotationConfigApplicationContext(ParallelJoinConfig.class);
        var info = FlowDefinitionScanner.scanByFlowType(ctx).get("parallel-test");
        StepRegistry<TestFlow> registry = new StepRegistry<>(info.handlers());

        List<StepHandler<TestFlow>> group = registry.getParallelGroup("processing");
        assertEquals(2, group.size());

        var names = group.stream().map(StepHandler::getStepName).toList();
        assertTrue(names.contains("PROCESS_A"));
        assertTrue(names.contains("PROCESS_B"));

        ctx.close();
    }

    // ========== Valid: Two parallel groups in sequence ==========

    @Flow(name = "double-parallel")
    static class DoubleParallelFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void start(TestFlow flow) {}

        @Step(order = 2)
        @Parallel(group = "group1")
        public void g1a(TestFlow flow) {}

        @Step(order = 2)
        @Parallel(group = "group1")
        public void g1b(TestFlow flow) {}

        @Step(order = 3)
        @JoinOn(group = "group1")
        public void join1(TestFlow flow) {}

        @Step(order = 4)
        @Parallel(group = "group2")
        public void g2a(TestFlow flow) {}

        @Step(order = 4)
        @Parallel(group = "group2")
        public void g2b(TestFlow flow) {}

        @Step(order = 5)
        @JoinOn(group = "group2")
        public void join2(TestFlow flow) {}

        @Step(order = 6)
        public void end(TestFlow flow) {}
    }

    @Configuration
    static class DoubleParallelConfig {
        @Bean DoubleParallelFlow flow() { return new DoubleParallelFlow(); }
    }

    @Test
    void doubleParallel_correctStepOrder() {
        var ctx = new AnnotationConfigApplicationContext(DoubleParallelConfig.class);
        var info = FlowDefinitionScanner.scanByFlowType(ctx).get("double-parallel");
        StepRegistry<TestFlow> registry = new StepRegistry<>(info.handlers());

        assertEquals(8, info.handlers().size());
        assertEquals("START", registry.getFirstStep());

        // Group 1: G1A, G1B → JOIN1
        assertEquals(2, registry.getStepsAtSameOrder("G1A").size());
        assertEquals("JOIN1", registry.getNextStep("G1A"));

        // Group 2: G2A, G2B → JOIN2
        assertEquals(2, registry.getStepsAtSameOrder("G2A").size());
        assertEquals("JOIN2", registry.getNextStep("G2A"));

        // Final: END
        assertEquals("END", registry.getNextStep("JOIN2"));
        assertNull(registry.getNextStep("END"));

        ctx.close();
    }

    // ========== Invalid: @JoinOn references non-existent group ==========

    @Flow(name = "bad-join")
    static class BadJoinFlow extends FlowDefinition<TestFlow> {
        @Step(order = 1)
        public void step1(TestFlow flow) {}

        @Step(order = 2)
        @JoinOn(group = "nonexistent")
        public void badJoin(TestFlow flow) {}
    }

    @Configuration
    static class BadJoinConfig {
        @Bean BadJoinFlow flow() { return new BadJoinFlow(); }
    }

    @Test
    void joinOnNonexistentGroup_failsStartup() {
        var ctx = new AnnotationConfigApplicationContext(BadJoinConfig.class);
        assertThrows(IllegalStateException.class,
                () -> FlowDefinitionScanner.scanByFlowType(ctx));
        ctx.close();
    }

    // ========== FlowType name derivation ==========

    @Test
    void flowTypeName_derivedFromAnnotation() {
        var ctx = new AnnotationConfigApplicationContext(ParallelJoinConfig.class);
        var flows = FlowDefinitionScanner.scanByFlowType(ctx);
        assertTrue(flows.containsKey("parallel-test"));
        ctx.close();
    }

    @Test
    void flowTypeName_derivedFromClassName() {
        var ctx = new AnnotationConfigApplicationContext(DoubleParallelConfig.class);
        var flows = FlowDefinitionScanner.scanByFlowType(ctx);
        assertTrue(flows.containsKey("double-parallel"));
        ctx.close();
    }
}
