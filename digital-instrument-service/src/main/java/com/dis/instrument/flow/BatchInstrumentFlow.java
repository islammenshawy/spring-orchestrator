package com.dis.instrument.flow;

import com.dis.instrument.model.InstrumentType;
import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Batch parent flow — starts multiple enigio-instrument child flows
 * and waits for all to complete.
 *
 * Tests child workflow lifecycle: async start, await, cancellation cascade,
 * parent notification on child completion.
 *
 * <pre>
 * POST /flows/batch-instrument
 * {
 *   "references": ["BATCH-001", "BATCH-002", "BATCH-003"],
 *   "title": "Batch Promissory Notes",
 *   "instrumentType": "PROMISSORY_NOTE",
 *   "documentCode": "NEG"
 * }
 * </pre>
 */
@Slf4j
@Component
@Flow(name = "batch-instrument")
public class BatchInstrumentFlow extends FlowDefinition<BatchInstrumentEntity> {

    @Step(order = 1)
    public void startChildren(BatchInstrumentEntity batch) {
        log.info("[{}] Starting {} child instrument flows", batch.getId(), batch.getReferences().size());

        for (String ref : batch.getReferences()) {
            EnigioInstrumentEntity child = new EnigioInstrumentEntity();
            child.setCorrelationId(ref); // deterministic — survives crash mid-loop
            child.setReference(ref);
            child.setTitle(batch.getTitle());
            child.setInstrumentType(batch.getInstrumentType() != null
                    ? batch.getInstrumentType() : InstrumentType.PROMISSORY_NOTE);
            child.setDocumentCode(
                    com.dis.instrument.model.DocumentCode.valueOf(
                            batch.getDocumentCode() != null ? batch.getDocumentCode() : "NEG"));
            var signer = new com.dis.instrument.model.Signer();
            signer.setName("Batch Signer");
            signer.setEmail("signer@batch.com");
            signer.setPhone("+46700000001");
            signer.setCapacity("CEO");
            signer.setOrganisation("Batch Corp");
            signer.setOrder(1);
            child.setSigners(List.of(signer));

            var recipient = new com.dis.instrument.model.Recipient();
            recipient.setName("Batch Recipient");
            recipient.setEmail("recipient@batch.com");
            child.setRecipient(recipient);

            startChildFlowAsync(batch, "enigio-instrument", child, Duration.ofHours(24));
        }

        log.info("[{}] Started {} children, awaiting completion", batch.getId(), batch.getChildFlowIds().size());
        awaitChildren(batch, Duration.ofHours(24));
    }

    @Step(order = 2)
    public void batchComplete(BatchInstrumentEntity batch) {
        // Count results from children
        int completed = 0;
        int failed = 0;
        if (batch.getChildFlowIds() != null) {
            for (String childId : batch.getChildFlowIds()) {
                // Status would be checked via repository in a real implementation
                completed++; // If we got here, awaitChildren passed — all done
            }
        }
        batch.setCompletedCount(completed);
        log.info("[{}] Batch complete — {} children processed", batch.getId(), completed);
    }
}
