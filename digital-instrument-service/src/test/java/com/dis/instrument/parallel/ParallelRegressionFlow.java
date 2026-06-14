package com.dis.instrument.parallel;

import com.orchestrator.starter.annotation.Flow;
import com.orchestrator.starter.annotation.JoinOn;
import com.orchestrator.starter.annotation.Parallel;
import com.orchestrator.starter.annotation.Step;
import com.orchestrator.starter.flow.FlowDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Self-contained parallel/join flow used as an end-to-end regression fixture for
 * the orchestrator's @Parallel/@JoinOn advancement. Mirrors a real flow shape but
 * with no external dependencies so it runs fast and deterministically:
 *
 *   INIT (1)
 *   LEFT (2) ‖ RIGHT (2)     group "work"   → joined by MERGE (3)
 *   NOTIFY (4) ‖ ARCHIVE (4) group "deliver" → joined by FINALIZE (5)
 *
 * Each join asserts it observed BOTH siblings (mergedResult = left+right;
 * finalResult = notify+archive), which catches both the completion-order stall
 * (flow never reaching the join) and any concurrent-save clobber of a sibling's
 * result. Registered as a @Component in the test sources so @SpringBootTest scans
 * it; it adds a flow type without touching the production flows.
 */
@Slf4j
@Component
@Flow(name = "parallel-regression")
public class ParallelRegressionFlow extends FlowDefinition<ParallelRegressionEntity> {

    @Step(order = 1, name = "INIT")
    public void init(ParallelRegressionEntity flow) {
        flow.setInitResult("init-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 2, name = "LEFT")
    @Parallel(group = "work")
    public void left(ParallelRegressionEntity flow) {
        flow.setLeftResult("left-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 2, name = "RIGHT")
    @Parallel(group = "work")
    public void right(ParallelRegressionEntity flow) {
        flow.setRightResult("right-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 3, name = "MERGE")
    @JoinOn(group = "work")
    public void merge(ParallelRegressionEntity flow) {
        if (flow.getLeftResult() == null || flow.getRightResult() == null) {
            throw new IllegalStateException("MERGE join ran before both siblings: left="
                    + flow.getLeftResult() + " right=" + flow.getRightResult());
        }
        flow.setMergedResult(flow.getLeftResult() + "+" + flow.getRightResult());
        checkpoint(flow);
    }

    @Step(order = 4, name = "NOTIFY")
    @Parallel(group = "deliver")
    public void notifyStep(ParallelRegressionEntity flow) {
        flow.setNotifyResult("notify-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 4, name = "ARCHIVE")
    @Parallel(group = "deliver")
    public void archive(ParallelRegressionEntity flow) {
        flow.setArchiveResult("archive-" + UUID.randomUUID().toString().substring(0, 8));
        checkpoint(flow);
    }

    @Step(order = 5, name = "FINALIZE")
    @JoinOn(group = "deliver")
    public void finalize(ParallelRegressionEntity flow) {
        if (flow.getNotifyResult() == null || flow.getArchiveResult() == null) {
            throw new IllegalStateException("FINALIZE join ran before both siblings: notify="
                    + flow.getNotifyResult() + " archive=" + flow.getArchiveResult());
        }
        flow.setFinalResult(flow.getNotifyResult() + "+" + flow.getArchiveResult());
        checkpoint(flow);
    }
}
