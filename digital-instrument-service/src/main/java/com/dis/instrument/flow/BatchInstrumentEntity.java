package com.dis.instrument.flow;

import com.dis.instrument.model.InstrumentType;
import com.orchestrator.starter.domain.AbstractFlow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * Parent flow entity for batch instrument processing.
 * Starts multiple child enigio-instrument flows and waits for all to complete.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "batch_instrument_flows")
public class BatchInstrumentEntity extends AbstractFlow {

    private List<String> references;
    private String title;
    private InstrumentType instrumentType;
    private String documentCode;
    private int completedCount;
    private int failedCount;
}
