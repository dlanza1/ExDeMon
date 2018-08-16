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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lastUpdateMetricTime == null) ? 0 : lastUpdateMetricTime.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VariableStatus other = (VariableStatus) obj;
        if (lastUpdateMetricTime == null) {
            if (other.lastUpdateMetricTime != null)
                return false;
        } else if (!lastUpdateMetricTime.equals(other.lastUpdateMetricTime))
            return false;
        return true;
    }
    
}
