package ch.cern.exdemon.metrics.defined.equation.var;

import java.io.Serializable;
import java.time.Instant;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class VariableStatus implements Serializable {

    private static final long serialVersionUID = -1238303955426246795L;

    @Getter
    @Setter
    private Instant lastUpdateMetricTime;
    
    public VariableStatus() {
        lastUpdateMetricTime = Instant.EPOCH;
    }
    
}
