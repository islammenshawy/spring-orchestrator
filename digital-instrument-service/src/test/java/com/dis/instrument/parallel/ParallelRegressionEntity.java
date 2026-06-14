package com.dis.instrument.parallel;

import com.orchestrator.starter.domain.AbstractFlow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Flow entity for the parallel-join regression fixture. Domain fields only —
 * each step records its own result so the join steps can prove they observed
 * BOTH parallel siblings' state.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "parallel_regression_flows")
public class ParallelRegressionEntity extends AbstractFlow {

    private String initResult;

    // group "work" (parallel) → joined by MERGE
    private String leftResult;
    private String rightResult;
    private String mergedResult;

    // group "deliver" (parallel) → joined by FINALIZE
    private String notifyResult;
    private String archiveResult;
    private String finalResult;
}
