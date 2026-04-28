package com.example.enigio.flow;

import com.orchestrator.starter.domain.AbstractFlow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Flow entity for parallel/join testing.
 * Tests: sequential → parallel group → join → sequential → parallel group → join → complete
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "parallel_flows")
public class ParallelFlow extends AbstractFlow {

    private String title;

    // Step 1: INIT
    private String initResult;

    // Step 2a + 2b: parallel group "validation"
    private String validationResult;
    private String enrichmentResult;

    // Step 3: JOIN_VALIDATION (after both 2a + 2b complete)
    private String mergedResult;

    // Step 4a + 4b: parallel group "delivery"
    private String notificationResult;
    private String archiveResult;

    // Step 5: FINALIZE
    private String finalResult;
}
