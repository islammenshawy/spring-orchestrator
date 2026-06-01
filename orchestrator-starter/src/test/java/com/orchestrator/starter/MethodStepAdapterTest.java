package com.orchestrator.starter;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.flow.FlowDefinition;
import com.orchestrator.starter.flow.MethodStepAdapter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class MethodStepAdapterTest {

    // ========== Test flow entity ==========

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "adapter_test_flows")
    static class TestAdapterFlow extends AbstractFlow {
        private String result;
        private boolean compensated;
        private boolean cancelled;
    }

    // ========== Flow definitions for various test scenarios ==========

    /** Minimal flow: no class-level annotations, no compensate/cancel */
    static class MinimalFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        public void createDocument(TestAdapterFlow flow) {
            flow.setResult("created");
        }

        @Step(order = 2, name = "CUSTOM_NAME")
        public void stepWithExplicitName(TestAdapterFlow flow) {
            flow.setResult("custom");
        }
    }

    /** Flow with compensation and cancellation handlers */
    static class CompensableFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        public void createDocument(TestAdapterFlow flow) {
            flow.setResult("created");
        }

        @Compensate(step = "createDocument")
        public void undoCreateDocument(TestAdapterFlow flow) {
            flow.setCompensated(true);
            flow.setResult(null);
        }

        @Step(order = 2)
        public void registerDocument(TestAdapterFlow flow) {
            flow.setResult("registered");
        }

        @Compensate(step = "registerDocument")
        public void undoRegisterDocument(TestAdapterFlow flow) {
            flow.setCompensated(true);
        }

        @OnCancel(step = "registerDocument")
        public void cancelRegisterDocument(TestAdapterFlow flow) {
            flow.setCancelled(true);
        }
    }

    /** Flow with method-level retry/fail/recover annotations */
    @RetryOn(httpStatus = {500, 502})
    @FailOn(httpStatus = {400})
    @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
    static class ClassAnnotatedFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        public void stepUsingClassAnnotations(TestAdapterFlow flow) {
            flow.setResult("class-level");
        }

        @Step(order = 2)
        @RetryOn(httpStatus = {503, 429})
        @FailOn(httpStatus = {403, 404})
        @RecoverOn(httpStatus = 422, action = RecoverAction.SKIP)
        public void stepWithMethodAnnotations(TestAdapterFlow flow) {
            flow.setResult("method-level");
        }

        @Step(order = 3)
        @RecoverOn(httpStatus = 409, action = RecoverAction.SKIP)
        @RecoverOn(httpStatus = 422, message = "already signed", action = RecoverAction.SKIP)
        public void stepWithMultipleRecoverOn(TestAdapterFlow flow) {
            flow.setResult("multi-recover");
        }
    }

    /** Flow with parallel and join annotations */
    static class ParallelFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        @Parallel(group = "prep")
        public void uploadAttachment(TestAdapterFlow flow) {
            flow.setResult("uploaded");
        }

        @Step(order = 1)
        @Parallel(group = "prep")
        public void requestSignature(TestAdapterFlow flow) {
            flow.setResult("signature-requested");
        }

        @Step(order = 2)
        @JoinOn(group = "prep")
        public void verifyBoth(TestAdapterFlow flow) {
            flow.setResult("verified");
        }

        @Step(order = 3)
        public void finalStep(TestAdapterFlow flow) {
            flow.setResult("done");
        }
    }

    /** Flow with a step that throws */
    static class ThrowingFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        public void failingStep(TestAdapterFlow flow) {
            throw new IllegalArgumentException("step failed");
        }

        @Step(order = 2)
        public void checkedExceptionStep(TestAdapterFlow flow) throws Exception {
            throw new Exception("checked failure");
        }
    }

    /** Flow with compensation that throws */
    static class FailingCompensationFlowDef extends FlowDefinition<TestAdapterFlow> {

        @Step(order = 1)
        public void doWork(TestAdapterFlow flow) {
            flow.setResult("worked");
        }

        @Compensate(step = "doWork")
        public void undoWork(TestAdapterFlow flow) {
            throw new RuntimeException("compensation boom");
        }

        @OnCancel(step = "doWork")
        public void cancelWork(TestAdapterFlow flow) {
            throw new IllegalStateException("cancel boom");
        }
    }

    // ========== Helper: create adapter from method name ==========

    private MethodStepAdapter<TestAdapterFlow> adapterFor(Object flowDef, String methodName) {
        for (Method m : flowDef.getClass().getDeclaredMethods()) {
            if (m.getName().equals(methodName)) {
                Step step = m.getAnnotation(Step.class);
                if (step != null) {
                    return new MethodStepAdapter<>(flowDef, m, step);
                }
            }
        }
        throw new IllegalArgumentException("No @Step method named " + methodName);
    }

    // ========== Step Name Resolution ==========

    @Nested
    class StepNameResolution {

        @Test
        void camelCase_convertsToUpperSnakeCase() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertEquals("CREATE_DOCUMENT", adapter.getStepName());
        }

        @Test
        void explicitName_usedAsIs() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithExplicitName");

            assertEquals("CUSTOM_NAME", adapter.getStepName());
        }

        @Test
        void singleWord_convertsToUpperCase() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            // "verifyBoth" -> "VERIFY_BOTH"
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "verifyBoth");

            assertEquals("VERIFY_BOTH", adapter.getStepName());
        }
    }

    // ========== getOrder ==========

    @Nested
    class OrderTests {

        @Test
        void order_returnsAnnotationValue() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertEquals(1, adapter.getOrder());
        }

        @Test
        void order_secondStep_returnsTwo() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithExplicitName");

            assertEquals(2, adapter.getOrder());
        }
    }

    // ========== execute ==========

    @Nested
    class ExecuteTests {

        @Test
        void execute_invokesMethodOnFlowDefinition() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("exec-1");

            adapter.execute(flow);

            assertEquals("created", flow.getResult());
        }

        @Test
        void execute_runtimeException_rethrown() {
            ThrowingFlowDef flowDef = new ThrowingFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "failingStep");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("exec-throw-1");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> adapter.execute(flow));

            assertEquals("step failed", ex.getMessage());
        }

        @Test
        void execute_checkedException_wrappedInRuntimeException() {
            ThrowingFlowDef flowDef = new ThrowingFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "checkedExceptionStep");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("exec-checked-1");

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> adapter.execute(flow));

            assertInstanceOf(Exception.class, ex.getCause());
            assertEquals("checked failure", ex.getCause().getMessage());
        }
    }

    // ========== compensate ==========

    @Nested
    class CompensateTests {

        @Test
        void compensate_invokesCompensateMethod() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("comp-1");
            flow.setResult("created");

            adapter.compensate(flow);

            assertTrue(flow.isCompensated());
            assertNull(flow.getResult());
        }

        @Test
        void compensate_noCompensateMethod_skipsWithoutError() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("comp-none-1");

            // Should not throw — just logs a warning
            assertDoesNotThrow(() -> adapter.compensate(flow));
            assertFalse(flow.isCompensated());
        }

        @Test
        void compensate_throwsException_wrappedInRuntimeException() {
            FailingCompensationFlowDef flowDef = new FailingCompensationFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "doWork");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("comp-fail-1");

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> adapter.compensate(flow));

            assertTrue(ex.getMessage().contains("Compensation failed for step"));
            assertEquals("compensation boom", ex.getCause().getMessage());
        }
    }

    // ========== hasCompensation ==========

    @Nested
    class HasCompensationTests {

        @Test
        void hasCompensation_withCompensateMethod_returnsTrue() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertTrue(adapter.hasCompensation());
        }

        @Test
        void hasCompensation_withoutCompensateMethod_returnsFalse() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertFalse(adapter.hasCompensation());
        }
    }

    // ========== cancel ==========

    @Nested
    class CancelTests {

        @Test
        void cancel_withOnCancelMethod_invokesOnCancel() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "registerDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("cancel-oncancel-1");

            adapter.cancel(flow);

            assertTrue(flow.isCancelled());
            assertFalse(flow.isCompensated(), "Should use @OnCancel, not @Compensate");
        }

        @Test
        void cancel_noOnCancel_fallsBackToCompensate() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("cancel-fallback-1");
            flow.setResult("created");

            adapter.cancel(flow);

            // Should fall back to @Compensate
            assertTrue(flow.isCompensated());
            assertNull(flow.getResult());
        }

        @Test
        void cancel_noOnCancelOrCompensate_skipsWithoutError() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("cancel-none-1");

            assertDoesNotThrow(() -> adapter.cancel(flow));
        }

        @Test
        void cancel_onCancelThrows_wrappedInRuntimeException() {
            FailingCompensationFlowDef flowDef = new FailingCompensationFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "doWork");
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("cancel-fail-1");

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> adapter.cancel(flow));

            assertTrue(ex.getMessage().contains("Cancellation failed for step"));
            assertEquals("cancel boom", ex.getCause().getMessage());
        }

        @Test
        void hasCancellation_withOnCancel_returnsTrue() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "registerDocument");

            assertTrue(adapter.hasCancellation());
        }

        @Test
        void hasCancellation_withOnlyCompensate_returnsTrue() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            // hasCancellation returns true if either @OnCancel or @Compensate exists
            assertTrue(adapter.hasCancellation());
        }

        @Test
        void hasCancellation_noHandlers_returnsFalse() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertFalse(adapter.hasCancellation());
        }
    }

    // ========== Annotation Inheritance: RetryOn, FailOn, RecoverOn ==========

    @Nested
    class AnnotationInheritanceTests {

        @Test
        void getRetryOn_methodLevel_overridesClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithMethodAnnotations");

            RetryOn retryOn = adapter.getRetryOn();
            assertNotNull(retryOn);
            assertArrayEquals(new int[]{503, 429}, retryOn.httpStatus());
        }

        @Test
        void getRetryOn_noMethodLevel_fallsBackToClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepUsingClassAnnotations");

            RetryOn retryOn = adapter.getRetryOn();
            assertNotNull(retryOn);
            assertArrayEquals(new int[]{500, 502}, retryOn.httpStatus());
        }

        @Test
        void getRetryOn_noAnnotationAnywhere_returnsNull() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertNull(adapter.getRetryOn());
        }

        @Test
        void getFailOn_methodLevel_overridesClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithMethodAnnotations");

            FailOn failOn = adapter.getFailOn();
            assertNotNull(failOn);
            assertArrayEquals(new int[]{403, 404}, failOn.httpStatus());
        }

        @Test
        void getFailOn_noMethodLevel_fallsBackToClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepUsingClassAnnotations");

            FailOn failOn = adapter.getFailOn();
            assertNotNull(failOn);
            assertArrayEquals(new int[]{400}, failOn.httpStatus());
        }

        @Test
        void getFailOn_noAnnotationAnywhere_returnsNull() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            assertNull(adapter.getFailOn());
        }

        @Test
        void getRecoverOns_methodLevel_overridesClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithMethodAnnotations");

            RecoverOn[] recoverOns = adapter.getRecoverOns();
            assertEquals(1, recoverOns.length);
            assertEquals(422, recoverOns[0].httpStatus());
        }

        @Test
        void getRecoverOns_noMethodLevel_fallsBackToClassLevel() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepUsingClassAnnotations");

            RecoverOn[] recoverOns = adapter.getRecoverOns();
            assertEquals(1, recoverOns.length);
            assertEquals(409, recoverOns[0].httpStatus());
            assertEquals(RecoverAction.SKIP, recoverOns[0].action());
        }

        @Test
        void getRecoverOns_noAnnotationAnywhere_returnsEmptyArray() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            RecoverOn[] recoverOns = adapter.getRecoverOns();
            assertEquals(0, recoverOns.length);
        }

        @Test
        void getRecoverOns_multipleMethodLevel_returnsAll() {
            ClassAnnotatedFlowDef flowDef = new ClassAnnotatedFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "stepWithMultipleRecoverOn");

            RecoverOn[] recoverOns = adapter.getRecoverOns();
            assertEquals(2, recoverOns.length);

            // Verify both are present (order may vary)
            boolean has409 = false, has422 = false;
            for (RecoverOn r : recoverOns) {
                if (r.httpStatus() == 409) has409 = true;
                if (r.httpStatus() == 422) {
                    has422 = true;
                    assertEquals("already signed", r.message());
                }
            }
            assertTrue(has409, "Should have RecoverOn for 409");
            assertTrue(has422, "Should have RecoverOn for 422");
        }
    }

    // ========== Parallel / JoinOn ==========

    @Nested
    class ParallelJoinTests {

        @Test
        void isParallel_withParallelAnnotation_returnsTrue() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "uploadAttachment");

            assertTrue(adapter.isParallel());
            assertEquals("prep", adapter.getParallelGroup());
        }

        @Test
        void isParallel_withoutAnnotation_returnsFalse() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "finalStep");

            assertFalse(adapter.isParallel());
            assertNull(adapter.getParallelGroup());
        }

        @Test
        void isJoinPoint_withJoinOnAnnotation_returnsTrue() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "verifyBoth");

            assertTrue(adapter.isJoinPoint());
            assertEquals("prep", adapter.getJoinOnGroup());
        }

        @Test
        void isJoinPoint_withoutAnnotation_returnsFalse() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "finalStep");

            assertFalse(adapter.isJoinPoint());
            assertNull(adapter.getJoinOnGroup());
        }

        @Test
        void parallelStep_isNotJoinPoint() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "uploadAttachment");

            assertTrue(adapter.isParallel());
            assertFalse(adapter.isJoinPoint());
        }

        @Test
        void joinPoint_isNotParallel() {
            ParallelFlowDef flowDef = new ParallelFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "verifyBoth");

            assertTrue(adapter.isJoinPoint());
            assertFalse(adapter.isParallel());
        }
    }

    // ========== Edge cases ==========

    @Nested
    class EdgeCases {

        @Test
        void multipleStepsOnSameFlowDef_eachHasCorrectName() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter1 = adapterFor(flowDef, "createDocument");
            MethodStepAdapter<TestAdapterFlow> adapter2 = adapterFor(flowDef, "registerDocument");

            assertEquals("CREATE_DOCUMENT", adapter1.getStepName());
            assertEquals("REGISTER_DOCUMENT", adapter2.getStepName());
        }

        @Test
        void stepAdapterRetainsFlowDefinitionReference() {
            MinimalFlowDef flowDef = new MinimalFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter = adapterFor(flowDef, "createDocument");

            // Execute should use the same flow definition instance
            TestAdapterFlow flow = new TestAdapterFlow();
            flow.setId("ref-1");
            adapter.execute(flow);
            assertEquals("created", flow.getResult());
        }

        @Test
        void compensateForOneStep_doesNotAffectOtherStep() {
            CompensableFlowDef flowDef = new CompensableFlowDef();
            MethodStepAdapter<TestAdapterFlow> adapter1 = adapterFor(flowDef, "createDocument");
            MethodStepAdapter<TestAdapterFlow> adapter2 = adapterFor(flowDef, "registerDocument");

            assertTrue(adapter1.hasCompensation());
            assertTrue(adapter2.hasCompensation());

            // Verify each step has its own compensation
            TestAdapterFlow flow1 = new TestAdapterFlow();
            flow1.setId("indep-1");
            flow1.setResult("created");
            adapter1.compensate(flow1);
            assertTrue(flow1.isCompensated());
            assertNull(flow1.getResult()); // undoCreateDocument clears result

            TestAdapterFlow flow2 = new TestAdapterFlow();
            flow2.setId("indep-2");
            flow2.setResult("registered");
            adapter2.compensate(flow2);
            assertTrue(flow2.isCompensated());
            assertEquals("registered", flow2.getResult()); // undoRegisterDocument does NOT clear result
        }
    }
}
