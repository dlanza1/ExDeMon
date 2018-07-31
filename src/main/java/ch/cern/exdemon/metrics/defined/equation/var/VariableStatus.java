package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;

import ch.cern.spark.status.StatusValue;
import lombok.Getter;
import lombok.Setter;

public class VariableStatus extends StatusValue {

    private static final long serialVersionUID = -1238303955426246795L;

    @Getter
    @Setter
    private Instant lastUpdateMetricTime;
    
    public VariableStatus() {
        lastUpdateMetricTime = Instant.EPOCH;
    }
    
}
