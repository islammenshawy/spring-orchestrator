package com.orchestrator.starter;

import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import com.orchestrator.starter.outbox.OutboxEventRepository;
import com.orchestrator.starter.recovery.StaleFlowRecoveryService;
import tools.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class StaleFlowRecoveryTest {

    private KafkaTemplate kafkaTemplate;
    private OutboxEventRepository outboxRepo;

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "flow_a")
    static class FlowA extends AbstractFlow {}

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Document(collection = "flow_b")
    static class FlowB extends AbstractFlow {}

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        outboxRepo = mock(OutboxEventRepository.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepo.countByFlowIdAndPublishedFalse(anyString())).thenReturn(0L);
    }

    @Test
    void recoverStaleFlows_multiFlow_recoversAllFlowTypes() {
        // Two flow types with separate repos and topics
        OrchestratorFlowRepository<FlowA> repoA = mock(OrchestratorFlowRepository.class);
        OrchestratorFlowRepository<FlowB> repoB = mock(OrchestratorFlowRepository.class);

        FlowA staleA = new FlowA();
        staleA.setId("a-1");
        staleA.setCurrentStep("STEP_1");
        staleA.setStatus(FlowStatus.IN_PROGRESS);
        staleA.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        FlowB staleB = new FlowB();
        staleB.setId("b-1");
        staleB.setCurrentStep("CHARGE");
        staleB.setStatus(FlowStatus.IN_PROGRESS);
        staleB.setUpdatedAt(Instant.now().minus(20, ChronoUnit.MINUTES));

        when(repoA.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of(staleA));
        when(repoB.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of(staleB));

        FlowTypeDescriptor descA = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(FlowA.class)
                .commandTopic("enigio.commands").replyTopic("enigio.commands.replies")
                .replyEnabled(true).repository(repoA)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();
        FlowTypeDescriptor descB = FlowTypeDescriptor.builder()
                .flowType("payment").entityClass(FlowB.class)
                .commandTopic("payment.commands").replyTopic("payment.commands.replies")
                .replyEnabled(true).repository(repoB)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(descA, descB));

        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), 15, outboxRepo);

        service.recoverStaleFlows();

        // Both flow types recovered — messages sent to their respective topics
        verify(kafkaTemplate).send(eq("enigio.commands"), anyString(), contains("STEP_1"));
        verify(kafkaTemplate).send(eq("payment.commands"), anyString(), contains("CHARGE"));
        verify(repoA).save(staleA);
        verify(repoB).save(staleB);
    }

    @Test
    void recoverStaleFlows_skipsFlowsWithPendingOutbox() {
        OrchestratorFlowRepository<FlowA> repoA = mock(OrchestratorFlowRepository.class);

        FlowA staleFlow = new FlowA();
        staleFlow.setId("a-1");
        staleFlow.setCurrentStep("STEP_1");
        staleFlow.setStatus(FlowStatus.IN_PROGRESS);

        when(repoA.findByStatusAndUpdatedAtBefore(eq(FlowStatus.IN_PROGRESS), any()))
                .thenReturn(List.of(staleFlow));
        // Has pending outbox events — pipeline is busy, not stuck
        when(outboxRepo.countByFlowIdAndPublishedFalse("a-1")).thenReturn(2L);

        FlowTypeDescriptor desc = FlowTypeDescriptor.builder()
                .flowType("enigio").entityClass(FlowA.class)
                .commandTopic("enigio.commands").replyTopic("enigio.commands.replies")
                .replyEnabled(true).repository(repoA)
                .orchestrator(mock(FlowOrchestrator.class))
                .build();

        FlowTypeRegistry registry = new FlowTypeRegistry(List.of(desc));
        StaleFlowRecoveryService service = new StaleFlowRecoveryService(
                registry, kafkaTemplate, new ObjectMapper(), 15, outboxRepo);

        service.recoverStaleFlows();

        // Not recovered — skipped due to pending outbox
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }
}
