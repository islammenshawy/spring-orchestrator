package com.example.enigio.flow;

import com.orchestrator.starter.annotation.*;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Parallel/join flow: tests two parallel groups with join points.
 *
 * Flow structure:
 *   Step 1: INIT (sequential)
 *   Step 2a: VALIDATE (parallel group "validation")
 *   Step 2b: ENRICH   (parallel group "validation")
 *   Step 3: MERGE_RESULTS (@JoinOn "validation" — waits for both 2a + 2b)
 *   Step 4a: NOTIFY  (parallel group "delivery")
 *   Step 4b: ARCHIVE (parallel group "delivery")
 *   Step 5: FINALIZE (@JoinOn "delivery" — waits for both 4a + 4b)
 */
@Slf4j
@Component
@Flow(name = "parallel-document")
public class ParallelDocumentFlow extends FlowDefinition<ParallelFlow> {

    @Step(order = 1, completedWhen = "initResult != null")
    public void init(ParallelFlow flow) {
        log.info("[Parallel] Init for flow {}", flow.getId());
        flow.setInitResult("initialized-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 2, completedWhen = "validationResult != null")
    @Parallel(group = "validation")
    public void validate(ParallelFlow flow) {
        log.info("[Parallel] Validate for flow {}", flow.getId());
        // Simulate validation work
        flow.setValidationResult("valid-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 2, completedWhen = "enrichmentResult != null")
    @Parallel(group = "validation")
    public void enrich(ParallelFlow flow) {
        log.info("[Parallel] Enrich for flow {}", flow.getId());
        // Simulate enrichment work
        flow.setEnrichmentResult("enriched-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 3, completedWhen = "mergedResult != null")
    @JoinOn(group = "validation")
    public void mergeResults(ParallelFlow flow) {
        log.info("[Parallel] Merge for flow {} (validation={}, enrichment={})",
                flow.getId(), flow.getValidationResult(), flow.getEnrichmentResult());
        flow.setMergedResult(flow.getValidationResult() + "+" + flow.getEnrichmentResult());
        checkpoint(flow);
    }

    @Step(order = 4, completedWhen = "notificationResult != null")
    @Parallel(group = "delivery")
    public void notify(ParallelFlow flow) {
        log.info("[Parallel] Notify for flow {}", flow.getId());
        flow.setNotificationResult("notified-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 4, completedWhen = "archiveResult != null")
    @Parallel(group = "delivery")
    public void archive(ParallelFlow flow) {
        log.info("[Parallel] Archive for flow {}", flow.getId());
        flow.setArchiveResult("archived-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 5, completedWhen = "finalResult != null")
    @JoinOn(group = "delivery")
    public void finalize(ParallelFlow flow) {
        log.info("[Parallel] Finalize for flow {} (notification={}, archive={})",
                flow.getId(), flow.getNotificationResult(), flow.getArchiveResult());
        flow.setFinalResult("complete-" + UUID.randomUUID().toString().substring(0, 8));
    }
}
