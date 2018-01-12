package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.defined.equation.ComputationException;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;

@ClassNameAlias("aggregated-values")
public class AggregationValues extends StatusValue {

    private static final long serialVersionUID = -3277697169862561894L;
    
    private Map<Integer, DatedValue> values;

    private int max_aggregation_size;

    public AggregationValues(int max_aggregation_size) {
        this.max_aggregation_size = max_aggregation_size;
        
        values = new LinkedHashMap<>();
    }
    
    public void add(int hashCode, float f, Instant now) {
        add(hashCode, new FloatValue(f), now);   
    }
    
    public void add(int idHash, Value value, Instant instant) {
        value = Value.clone(value);

        // Removing the oldest entry if max size
        if (values.size() >= max_aggregation_size + 1)
            values.remove(values.keySet().iterator().next());

        values.put(idHash, new DatedValue(instant, value));
    }

    public Collection<DatedValue> getDatedValues() throws ComputationException {
        if(values.size() > max_aggregation_size)
            throw new ComputationException("Maximum aggregation size reached.");
        
        return values.values();        
    }

    public void purge(Instant oldestTime) {
        values.entrySet().removeIf(entry -> entry.getValue().getInstant().isBefore(oldestTime));
    }

    public void setMax_aggregation_size(int max_aggregation_size) {
        this.max_aggregation_size = max_aggregation_size;
    }
    
    @Override
    public String toString() {
        return "AggregationValues [values=" + values + ", max_aggregation_size=" + max_aggregation_size + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + max_aggregation_size;
        result = prime * result + ((values == null) ? 0 : values.hashCode());
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
        AggregationValues other = (AggregationValues) obj;
        if (max_aggregation_size != other.max_aggregation_size)
            return false;
        if (values == null) {
            if (other.values != null)
                return false;
        } else if (!values.equals(other.values))
            return false;
        return true;
    }
    
}
