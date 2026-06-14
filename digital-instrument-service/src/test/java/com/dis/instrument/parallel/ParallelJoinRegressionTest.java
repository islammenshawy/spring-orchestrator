package com.dis.instrument.parallel;

import com.orchestrator.starter.domain.FlowStatus;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression for the orchestrator's @Parallel/@JoinOn advancement,
 * driven through real Kafka + MongoDB. Requires Kafka + MongoDB running.
 *
 * The join-advance bug was completion-ORDER dependent: a parallel flow only
 * advanced past its join when the sibling pinned as currentStep happened to
 * finish LAST — otherwise it stalled (~50% of the time). Each run exercises
 * one random completion order; 6 sequential runs make it extremely unlikely
 * to miss both orders.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class ParallelJoinRegressionTest {

    @Autowired
    private FlowTypeRegistry flowTypeRegistry;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeAll
    static void waitForKafka() throws InterruptedException {
        Thread.sleep(3000);
    }

    @SuppressWarnings("unchecked")
    private ParallelRegressionEntity startAndWait(int i) {
        FlowOrchestrator<ParallelRegressionEntity> orch =
                (FlowOrchestrator<ParallelRegressionEntity>) flowTypeRegistry
                        .resolve("parallel-regression").getOrchestrator();
        ParallelRegressionEntity flow = new ParallelRegressionEntity();
        flow.setCorrelationId("parreg-" + System.nanoTime() + "-" + i);
        String id = orch.startFlow(flow).getId();

        // Poll until COMPLETED or timeout
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            ParallelRegressionEntity f = mongoTemplate.findById(
                    id, ParallelRegressionEntity.class, "parallel_regression_flows");
            if (f != null && f.getStatus() == FlowStatus.COMPLETED) return f;
            if (f != null && f.getStatus() == FlowStatus.FAILED) return f;
            try { Thread.sleep(500); } catch (InterruptedException e) { break; }
        }
        // Return whatever state we have
        return mongoTemplate.findById(id, ParallelRegressionEntity.class, "parallel_regression_flows");
    }

    @Test
    void parallelJoin_completesAcrossManyRuns_exercisingBothCompletionOrders() {
        int runs = 6;
        List<String> stalled = new ArrayList<>();

        for (int i = 0; i < runs; i++) {
            ParallelRegressionEntity f = startAndWait(i);
            if (f == null || f.getStatus() != FlowStatus.COMPLETED) {
                String detail = f != null
                        ? f.getStatus() + " @ " + f.getCurrentStep()
                        : "NOT_FOUND";
                stalled.add("run-" + i + " → " + detail);
                continue;
            }
            // Both joins must have combined their two siblings
            assertNotNull(f.getMergedResult(), "MERGE join lost a sibling for run " + i);
            assertTrue(f.getMergedResult().contains("+"),
                    "MERGE must combine LEFT+RIGHT (got: " + f.getMergedResult() + ")");
            assertNotNull(f.getFinalResult(), "FINALIZE join did not complete for run " + i);
            assertTrue(f.getFinalResult().contains("+"),
                    "FINALIZE must combine NOTIFY+ARCHIVE (got: " + f.getFinalResult() + ")");
        }

        assertTrue(stalled.isEmpty(),
                stalled.size() + "/" + runs + " parallel flows stalled at the join "
                        + "(completion-order regression): " + stalled);
    }
}
