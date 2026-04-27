package com.orchestrator.starter;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.flow.*;
import com.orchestrator.starter.kafka.OrchestratorKafkaConsumer;
import com.orchestrator.starter.idempotency.IdempotencyService;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class MultiFlowTest {

    // ========== FlowTypeRegistry ==========

    @Test
    void registry_singleFlow_resolveNullFlowType() {
        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(Object.class)
                .commandTopic("commands").replyTopic("commands.replies")
                .replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        // Null flowType falls back to single flow
        assertSame(desc, registry.resolve(null));
        assertSame(desc, registry.resolve(""));
        assertSame(desc, registry.getSingleOrThrow());
    }

    @Test
    void registry_multipleFlows_resolveByFlowType() {
        FlowTypeDescriptor enigio = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(String.class)
                .commandTopic("shared.commands").replyTopic("shared.commands.replies")
                .replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeDescriptor payment = FlowTypeDescriptor.builder()
                .flowType("payment").entityClass(Integer.class)
                .commandTopic("shared.commands").replyTopic("shared.commands.replies")
                .replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(enigio, payment));

        assertSame(enigio, registry.resolve("enigio"));
        assertSame(payment, registry.resolve("payment"));
        assertEquals(2, registry.size());
    }

    @Test
    void registry_multipleFlows_nullFlowTypeThrows() {
        FlowTypeDescriptor a = FlowTypeDescriptor.builder()
                .flowType("a").entityClass(String.class).commandTopic("t")
                .replyTopic("t.r").replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeDescriptor b = FlowTypeDescriptor.builder()
                .flowType("b").entityClass(Integer.class).commandTopic("t")
                .replyTopic("t.r").replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(a, b));

        // Null flowType with multiple flows should throw
        assertThrows(IllegalStateException.class, () -> registry.resolve(null));
        assertThrows(IllegalStateException.class, () -> registry.getSingleOrThrow());
    }

    @Test
    void registry_unknownFlowTypeThrows() {
        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(Object.class).commandTopic("t")
                .replyTopic("t.r").replyEnabled(true).orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));

        assertThrows(IllegalArgumentException.class, () -> registry.resolve("unknown"));
    }

    @Test
    void registry_duplicateFlowTypeThrows() {
        FlowTypeDescriptor a = FlowTypeDescriptor.builder()
                .flowType("dup").entityClass(String.class).commandTopic("t")
                .replyTopic("t.r").replyEnabled(true).build();
        FlowTypeDescriptor b = FlowTypeDescriptor.builder()
                .flowType("dup").entityClass(Integer.class).commandTopic("t")
                .replyTopic("t.r").replyEnabled(true).build();

        assertThrows(IllegalStateException.class,
                () -> new FlowTypeRegistry(List.of(a, b)));
    }

    // ========== Shared topics ==========

    @Test
    void registry_sharedTopics_collectsUnique() {
        FlowTypeDescriptor a = FlowTypeDescriptor.builder()
                .flowType("a").entityClass(String.class)
                .commandTopic("shared.commands").replyTopic("shared.commands.replies")
                .replyEnabled(true).build();
        FlowTypeDescriptor b = FlowTypeDescriptor.builder()
                .flowType("b").entityClass(Integer.class)
                .commandTopic("shared.commands").replyTopic("shared.commands.replies")
                .replyEnabled(true).build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(a, b));

        // Shared topic = only 1 unique command topic
        assertEquals(1, registry.getAllCommandTopics().size());
        assertEquals(1, registry.getAllReplyTopics().size());
        assertTrue(registry.getAllCommandTopics().contains("shared.commands"));
    }

    @Test
    void registry_perFlowTopics_collectsAll() {
        FlowTypeDescriptor a = FlowTypeDescriptor.builder()
                .flowType("a").entityClass(String.class)
                .commandTopic("enigio.commands").replyTopic("enigio.commands.replies")
                .replyEnabled(true).build();
        FlowTypeDescriptor b = FlowTypeDescriptor.builder()
                .flowType("b").entityClass(Integer.class)
                .commandTopic("payment.commands").replyTopic("payment.commands.replies")
                .replyEnabled(true).build();
        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(a, b));

        assertEquals(2, registry.getAllCommandTopics().size());
        assertEquals(2, registry.getAllReplyTopics().size());
    }

    // ========== Consumer routing ==========

    @Test
    void consumer_routesByFlowType() {
        FlowOrchestrator enigioOrch = mock(FlowOrchestrator.class);
        FlowOrchestrator paymentOrch = mock(FlowOrchestrator.class);

        FlowTypeDescriptor enigio = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(String.class)
                .commandTopic("shared").replyTopic("shared.replies")
                .replyEnabled(true).orchestrator(enigioOrch).build();
        FlowTypeDescriptor payment = FlowTypeDescriptor.builder()
                .flowType("payment").entityClass(Integer.class)
                .commandTopic("shared").replyTopic("shared.replies")
                .replyEnabled(true).orchestrator(paymentOrch).build();

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(enigio, payment));
        IdempotencyService idempotency = mock(IdempotencyService.class);
        when(idempotency.isProcessed(any())).thenReturn(false);

        OrchestratorKafkaConsumer consumer = new OrchestratorKafkaConsumer(
                registry, idempotency, new ObjectMapper(), true);

        // Enigio flow message → routes to enigio orchestrator
        String enigioMsg = "{\"flowId\":\"f1\",\"stepName\":\"CREATE\",\"eventId\":\"e1\",\"flowType\":\"enigio\"}";
        consumer.onStepCommand(enigioMsg, "shared", 0);
        verify(enigioOrch).executeStepOnly("f1", "CREATE");
        verify(paymentOrch, never()).executeStepOnly(any(), any());

        // Payment flow message → routes to payment orchestrator
        String paymentMsg = "{\"flowId\":\"f2\",\"stepName\":\"CHARGE\",\"eventId\":\"e2\",\"flowType\":\"payment\"}";
        consumer.onStepCommand(paymentMsg, "shared", 1);
        verify(paymentOrch).executeStepOnly("f2", "CHARGE");
    }

    // ========== FlowType derivation logic ==========

    @Test
    void deriveFlowType_fromClassName() {
        // Tests the same regex logic as FlowDefinitionScanner.deriveFlowType()
        assertEquals("enigio-document", derive("EnigioDocumentFlow"));
        assertEquals("payment", derive("PaymentFlow"));
        assertEquals("simple", derive("SimpleFlow"));
        assertEquals("my-long-name", derive("MyLongNameFlow"));
    }

    /** Same logic as FlowDefinitionScanner.deriveFlowType — remove "Flow" suffix, kebab-case */
    private String derive(String className) {
        String name = className;
        if (name.endsWith("Flow")) name = name.substring(0, name.length() - 4);
        return name.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase();
    }
}
